import "dotenv/config";
import Express from "express";
import bodyParser from "body-parser";
import dataRouter from "./routes/data.router";
import { PgService } from "./services/pg.service";
import { RedisService } from "./services/redis.service";

class App {
    public app: Express.Application;
    constructor() {
        this.app = Express();
        this.config();
    }
    private config() {
        let newApp = Express();
        this.app.use(bodyParser.json({ strict: false }));
        this.app.use(
            bodyParser.json({
                strict: false,
                limit: "20mb",
                type: "application/json",
            }),
        );
        newApp.use("/data", dataRouter);
        this.app.use(`/`, newApp);
    }
}

const app = new App().app;
const PORT = process.env.PORT || 3000;

app.listen(PORT, async () => {
    console.log(`Server running on port ${PORT}`);
    await PgService.initialisePgDb();
    await RedisService.connect();
});

export default app;
