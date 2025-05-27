import express from "express";
import bodyParser from "body-parser";
import { Storage } from "@google-cloud/storage";
import { DocumentProcessorServiceClient } from "@google-cloud/documentai";
import OpenAI from "openai";
import mongoose from "mongoose";
import dotenv from 'dotenv';
import path from "path";



dotenv.config({path: '/etc/secrets/resume-analyser-env'});

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

        const [result] = await documentaiClient.processDocument({
            name: nameProcessor,
            rawDocument: {
                content: contents.toString("base64"),
                mimeType: "application/pdf",
            },
        });

        const text = result.document?.text || "";
        const chunks = text.match(/.{1,1000}/g) || [];

        const embeddings = await Promise.all(
            chunks.map(chunk => openai.embeddings.create({
                model: "text-embedding-3-small",
                input: chunk
            }).then(res => res.data[0].embedding))
        );

        await ResumeVector.create({
            userId: name.split("/")[1],
            textChunks: chunks,
            embeddings: embeddings
        });

        res.status(200).send("Processed");
    } catch (err) {
        console.error(err);
        res.status(500).send("Error processing resume");
    }
});

app.get('/test', async (req, res) => {
    try {
        const bucket = 'hirebizz-resume-bucket-2';
        const name = 'test_resume.pdf'; // <-- replace with an actual file you uploaded to your GCS bucket

        const file = storage.bucket(bucket).file(name);
        const contents = (await file.download())[0];

        const [result] = await documentaiClient.processDocument({
            name: nameProcessor,
            rawDocument: {
                content: contents.toString("base64"),
                mimeType: "application/pdf",
            },
        });

        const text = result.document?.text || "";
        const chunks = text.match(/.{1,1000}/g) || [];

        const embeddings = await Promise.all(
            chunks.map(chunk => openai.embeddings.create({
                model: "text-embedding-3-small",
                input: chunk
            }).then(res => res.data[0].embedding))
        );

        await ResumeVector.create({
            userId: 'test-user-id',
            textChunks: chunks,
            embeddings: embeddings
        });

        res.json({
            message: 'Test resume processed successfully',
            textChunksCount: chunks.length,
            firstChunkTextSample: chunks[0]?.slice(0, 200),
            embeddingsCount: embeddings.length,
            firstEmbeddingSample: embeddings[0]?.slice(0, 10) // first 10 numbers of the first embedding vector
        });
    } catch (error) {
        console.error(error);
        res.status(500).send('Error during test resume processing');
    }
});


async function startServer() {
    try {
        await mongoose.connect(process.env.MONGO_URI);

        app.listen(PORT, () => console.log(`Listening on port ${PORT}`));
    } catch (err) {
        console.error("Failed to connect to MongoDB:", err);
        process.exit(1);
    }
}

startServer();