import express from "express";
import bodyParser from "body-parser";
import { Storage } from "@google-cloud/storage";
import { DocumentProcessorServiceClient } from "@google-cloud/documentai";
import OpenAI from "openai";
import mongoose from "mongoose";
import dotenv from 'dotenv';
import path from "path";

dotenv.config({ path: '/etc/secrets/resume-analyser-env' });

const app = express();
const PORT = process.env.PORT || 8080;
app.use(bodyParser.json());

console.log("🔐 Loading environment variables...");
console.log("🔧 Connecting to services...");

const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY
});

console.log("📄 Setting up Document AI processor...");
const nameProcessor = `projects/${process.env.GOOGLE_PROJECT_ID}/locations/${process.env.GOOGLE_REGION_LOCATION}/processors/${process.env.GOOGLE_RESUME_PARSER_PROCESSOR_ID}`;

const storage = new Storage();
const documentaiClient = new DocumentProcessorServiceClient();

const ResumeVector = mongoose.model("resumes", new mongoose.Schema({
    userId: String,
    textChunks: [String],
    embeddings: [[Number]],
}));


app.post("/", async (req, res) => {
    console.log("📩 Received a new Pub/Sub message.");

    let pubsubMessage;
    try {
        pubsubMessage = req.body.message;
        if (!pubsubMessage?.data) {
            console.error("❌ Missing 'data' field in Pub/Sub message.");
            return res.status(200).send("Invalid Pub/Sub message format.");
        }
    } catch (e) {
        return res.status(200).send("Malformed Pub/Sub message");
    }


    res.status(200).send("Received");


    (async () => {
        try {
            const dataBuffer = Buffer.from(pubsubMessage.data, "base64");
            console.log("📦 Decoded message:", dataBuffer.toString());

            const { bucket, name } = JSON.parse(dataBuffer.toString());
            console.log(`🗃️ File path extracted: bucket = ${bucket}, name = ${name}`);

            const file = storage.bucket(bucket).file(name);
            
            const [metadata] = await file.getMetadata();
            const isPremium = metadata.metadata?.premium === 'true';

            console.log("💰 Premium metadata found:", isPremium);

            
            if (!isPremium) {
                console.log("❌ Skipping resume processing — not a premium user.");
                return;
            }
            const contents = (await file.download())[0];
            console.log("📄 File downloaded from Cloud Storage");

            let result;
            try {
                [result] = await documentaiClient.processDocument({
                    name: nameProcessor,
                    rawDocument: {
                        content: contents.toString("base64"),
                        mimeType: "application/pdf",
                    },
                });
            } catch (docErr) {
                console.error("❌ Error processing document with Document AI:", docErr.message || docErr);
                return;
            }

            const text = result.document?.text || "";
            const chunks = text.match(/.{1,1000}/g) || [];
            console.log(`🧩 Document split into ${chunks.length} chunks`);

            const embeddings = await Promise.all(
                chunks.map(async (chunk, i) => {
                    try {
                        const res = await openai.embeddings.create({
                            model: "text-embedding-3-small",
                            input: chunk
                        });

                        const vector = res.data?.[0]?.embedding;


                        if (!Array.isArray(vector) || !vector.every(Number.isFinite)) {
                            console.warn(`⚠️ Chunk ${i} embedding is invalid, inserting empty array`);
                            return [];
                        }

                        console.log(`✅ Chunk ${i} embedded successfully`);
                        return vector;
                    } catch (err) {
                        console.error(`❌ Embedding error for chunk ${i}:`, err.message);
                        return [];
                    }
                })
            );

            const validEmbeddings = embeddings.filter(
                (vec) => Array.isArray(vec) && vec.length > 0 && vec.every(Number.isFinite)
            );

            if (validEmbeddings.length === 0) {
                console.warn("⚠️ No valid embeddings found. Skipping Mongo insert.");
                return;
            }



            const userId = name.split("/")[0];
            console.log("📦 Inserting document into MongoDB:", { userId });

            try {
                await ResumeVector.deleteMany({ userId });

                await ResumeVector.create({
                    userId,
                    textChunks: chunks,
                    embeddings: validEmbeddings
                });
                console.log("✅ Valid data inserted into MongoDB");

            } catch (err) {
                console.error("❌ Mongo insert failed or timed out:", err.message);
            }

        } catch (err) {
            console.error("💥 Unhandled error in background resume processing:", err.message || err);
        }
    })();
});

async function startServer() {
    try {
        console.log("🔌 Connecting to MongoDB...");
        await mongoose.connect(process.env.MONGO_URI);
        console.log("✅ Connected to MongoDB");

        app.listen(PORT, () => console.log(`🚀 Listening on port ${PORT}`));
    } catch (err) {
        console.error("❌ Failed to connect to MongoDB:", err.message);
        process.exit(1);
    }
}

startServer();
