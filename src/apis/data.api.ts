import * as express from "express";
import {
    UploadDataResponseModel,
    FetchDataResponseModel,
} from "src/models/data.response";
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

const fetchDataFile = async (
    req: express.Request,
    res: express.Response<GenericResponseModel<FetchDataResponseModel>>,
) => {
    try {
        const page = parseInt(req.query.page as string) || 1;
        const limit = parseInt(req.query.limit as string) || 10;

        console.log("fetchDataFile called with page:", page, "limit:", limit);
        if (page < 1 || limit < 1 || limit > 100) {
            return res.status(400).json({
                error: "Invalid pagination parameters. Page must be >= 1, limit must be between 1 and 100",
            });
        }

        const result = await DataService.fetchData(page, limit);
        return res.json({
            message: "Data retrieved successfully",
            data: result,
        });
    } catch (err: any) {
        console.error("fetchDataFile error", err);
        return res.status(500).json({ error: err.message || "internal error" });
    }
};

export const DataApi = {
    uploadDataFile,
    fetchDataFile,
};
