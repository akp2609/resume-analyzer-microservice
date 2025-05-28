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

const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY
});

const nameProcessor = `projects/${process.env.GOOGLE_PROJECT_ID}/locations/${process.env.GOOGLE_REGION_LOCATION}/processors/${process.env.GOOGLE_RESUME_PARSER_PROCESSOR_ID}`;

const storage = new Storage();
const documentaiClient = new DocumentProcessorServiceClient();

const ResumeVector = mongoose.model("resumes", new mongoose.Schema({
    userId: String,
    textChunks: [String],
    embeddings: [[Number]],
}));

app.post("/", async (req, res) => {
    try {
        const pubsubMessage = req.body.message;
        const dataBuffer = Buffer.from(pubsubMessage.data, "base64");
        const { bucket, name } = JSON.parse(dataBuffer.toString());

        const file = storage.bucket(bucket).file(name);
        const contents = (await file.download())[0];

        let result;
        try{
        [result] = await documentaiClient.processDocument({
            name: nameProcessor,
            rawDocument: {
                content: contents.toString("base64"),
                mimeType: "application/pdf",
            },
        });}
        catch (docErr) {
            console.error("❌ Error processing document with Document AI:", docErr.message || docErr);
            return res.status(200).send("Document processing failed, message discarded.");
        }
    


    const text = result.document?.text || "";
    const chunks = text.match(/.{1,1000}/g) || [];

    const embeddings = await Promise.all(
        chunks.map(chunk => openai.embeddings.create({
            model: "text-embedding-3-small",
            input: chunk
        }).then(res => res.data[0].embedding))
    );

    console.log("📦 Inserting document:", {
        userId: name.split("/")[1],
        textChunks: chunks,
        embeddings: embeddings
    });

    try {
        await ResumeVector.create({
            userId: name.split("/")[1],
            textChunks: chunks,
            embeddings: embeddings
        });
        console.log("✅ Data inserted into MongoDB");
    } catch (insertErr) {
        console.error("❌ Error inserting into MongoDB:", insertErr);
    }

    res.status(200).send("Processed");
} catch (err) {
    console.error(err);
    res.status(500).send("Error processing resume");
}
});


async function startServer() {
    try {
        await mongoose.connect(process.env.MONGO_URI, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        console.log("✅ Connected to MongoDB");

        app.listen(PORT, () => console.log(`🚀 Listening on port ${PORT}`));
    } catch (err) {
        console.error("❌ Failed to connect to MongoDB:", err.message);
        process.exit(1);
    }
}


startServer();