export interface UploadDataResponseModel {
    success: boolean;
}

export interface UserData {
    id: number;
    name: string;
    email: string;
    city: string;
}

export interface UserEvent {
    data: Array<{
        name: string;
        email: string;
        city: string;
    }>;
    timestamp: string;
}

export type UserRow = {
    name: string;
    email: string;
    city: string;
};

export interface FetchDataResponseModel {
    data: UserData[];
    metadata: {
        page: number;
        limit: number;
        total: number;
        totalPages: number;
    };
    cached: boolean;
}
