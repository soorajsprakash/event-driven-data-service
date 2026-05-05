import * as express from "express";
import * as jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET || "dev-secret";

export const authenticateToken = (
    req: express.Request,
    res: express.Response,
    next: express.NextFunction,
): void => {
    const authHeader = req.headers["authorization"];
    const token = authHeader?.startsWith("Bearer ") ? authHeader.slice(7) : null;

    if (!token) {
        res.status(401).json({ error: "Missing or malformed Authorization header" });
        return;
    }

    try {
        const payload = jwt.verify(token, JWT_SECRET);
        (req as any).user = payload;
        next();
    } catch {
        res.status(401).json({ error: "Invalid or expired token" });
    }
};
