import * as express from "express";
import multer from "multer";
import { DataApi } from "../apis/data.api";
import { authenticateToken } from "../middleware/auth.middleware";
import { validateUpload } from "../middleware/upload.validation";

const upload = multer({ storage: multer.memoryStorage() });

const router = express.Router();

router.post("/", upload.single("file"), validateUpload, DataApi.uploadDataFile);
router.get("/", authenticateToken, DataApi.fetchDataFile);

export default router;
