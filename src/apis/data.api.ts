import * as express from "express";
import { UploadDataResponseModel } from "src/models/data.response";
import { GenericResponseModel } from "src/models/generic.response";
import { DataService } from "../services/data.service";

const uploadDataFile = async (
    req: express.Request,
    res: express.Response<GenericResponseModel<UploadDataResponseModel>>,
) => {
    try {
        const file = req.file;
        if (!file) {
            return res
                .status(400)
                .json({ error: "No file uploaded under field 'file'" });
        }

        const result = await DataService.uploadCsv(file.buffer);
        return res.json({
            message: "Successfully uploaded the data file",
            data: result,
        });
    } catch (err: any) {
        console.error("uploadDataFile error", err);
        return res.status(500).json({ error: err.message || "internal error" });
    }
};

export const DataApi = {
    uploadDataFile,
};
