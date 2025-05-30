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
                chunks.map(chunk =>
                    openai.embeddings.create({
                        model: "text-embedding-3-small",
                        input: chunk
                    }).then(res => {
                        console.log("✅ Embedding created for chunk");
                        return res.data[0].embedding;
                    }).catch(err => {
                        console.error("❌ Failed to get embedding:", err.message);
                        return [];
                    })
                )
            );

            const userId = name.split("/")[0];
            console.log("📦 Inserting document into MongoDB:", { userId });

            try {
                await ResumeVector.deleteMany({userId});

                await ResumeVector.create({
                    userId,
                    textChunks: chunks,
                    embeddings
                }); 
                console.log("✅ Data inserted into MongoDB");
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
