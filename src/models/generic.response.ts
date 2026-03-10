export interface GenericResponseModel<T = any> {
    error?: string;
    message?: string;
    data?: T;
}
