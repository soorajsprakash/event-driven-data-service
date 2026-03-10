import * as express from "express";
import multer from "multer";
import { DataApi } from "../apis/data.api";

const upload = multer({ storage: multer.memoryStorage() });

const router = express.Router();

router.post("/", upload.single("file"), DataApi.uploadDataFile);

export default router;
