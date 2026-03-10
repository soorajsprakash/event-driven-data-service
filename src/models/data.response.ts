export interface UploadDataResponseModel {
    success: boolean;
}

export interface UserData {
    id: number;
    name: string;
    email: string;
    city: string;
}

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
