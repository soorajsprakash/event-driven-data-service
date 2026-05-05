import * as express from "express";
import Joi from "joi";

const MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024; // 5 MB

const fileSchema = Joi.object({
    mimetype: Joi.string()
        .valid("text/csv", "application/vnd.ms-excel", "text/plain")
        .required()
        .messages({ "any.only": "Only CSV files are accepted" }),
    size: Joi.number()
        .max(MAX_FILE_SIZE_BYTES)
        .required()
        .messages({ "number.max": "File size must not exceed 5 MB" }),
}).unknown(true);

export const validateUpload = (
    req: express.Request,
    res: express.Response,
    next: express.NextFunction,
): void => {
    if (!req.file) {
        res.status(400).json({ error: "No file uploaded under field 'file'" });
        return;
    }

    const { error } = fileSchema.validate(req.file);
    if (error) {
        res.status(400).json({ error: error.details[0].message });
        return;
    }

    next();
};
