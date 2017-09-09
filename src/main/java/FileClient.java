import com.google.common.reflect.TypeToken;
import com.microsoft.azure.CloudException;
import com.microsoft.rest.ServiceResponse;
import com.microsoft.rest.protocol.ResponseBuilder;
import com.microsoft.rest.protocol.SerializerAdapter;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.PUT;
import retrofit2.http.Path;
import retrofit2.http.Query;
import retrofit2.http.Streaming;
import rx.Observable;
import rx.functions.Func1;

import java.io.IOException;
import java.io.InputStream;

public class FileClient {

    private FileService service;
    private ResponseBuilder.Factory responseBuilderFactory;
    private SerializerAdapter<?> serializerAdapter;

    public FileClient(Retrofit retrofit, ResponseBuilder.Factory responseBuilderFactory, SerializerAdapter<?> serializerAdapter) {
        this.responseBuilderFactory = responseBuilderFactory;
        this.serializerAdapter = serializerAdapter;
        service = retrofit.create(FileService.class);
    }

    public interface FileService {
        @Headers({ "Content-Type: application/json; charset=utf-8", "x-ms-logging-context: fixtures.bodyfile.FileClient getFile" })
        @GET("downloads/{size}.dat")
        @Streaming
        Observable<Response<ResponseBody>> getFileAsync(@Path("size") String size);

        @Headers({ "Content-Type: application/json; charset=utf-8", "x-ms-logging-context: fixtures.bodyfile.FileClient getFile" })
        @GET("downloads/{size}.dat")
        @Streaming
        Call<ResponseBody> getFile(@Path("size") String size);

        @Headers({ "Content-Type: application/octet-stream", "x-ms-version: 2017-04-17" })
        @PUT("uploads/{blobName}?comp=block")
        @Streaming
        Observable<Response<ResponseBody>> uploadBlob(@Path("blobName") String blobName, @Query("blockid") String blockId, @Body RequestBody fileContent);

        @Headers({ "Content-Type: application/xml", "x-ms-version: 2017-04-17" })
        @PUT("uploads/{blobName}?comp=blocklist")
        Observable<Response<ResponseBody>> commitBlob(@Path("blobName") String blobName, @Body String blockListXml);
    }

    public InputStream getFile(String size) {
        return getFileWithServiceResponse(size).body();
    }

    public ServiceResponse<InputStream> getFileWithServiceResponse(String size) {
        try {
            return getFileDelegate(service.getFile(size).execute());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Observable<InputStream> getFileAsync(String size) {
        return getFileWithServiceResponseAsync(size).map(new Func1<ServiceResponse<InputStream>, InputStream>() {
            @Override
            public InputStream call(ServiceResponse<InputStream> response) {
                return response.body();
            }
        });
    }

    public Observable<ServiceResponse<InputStream>> getFileWithServiceResponseAsync(String size) {
        return service.getFileAsync(size)
                .flatMap(new Func1<Response<ResponseBody>, Observable<ServiceResponse<InputStream>>>() {
                    @Override
                    public Observable<ServiceResponse<InputStream>> call(Response<ResponseBody> response) {
                        try {
                            ServiceResponse<InputStream> clientResponse = getFileDelegate(response);
                            return Observable.just(clientResponse);
                        } catch (Throwable t) {
                            return Observable.error(t);
                        }
                    }
                });
    }

    private ServiceResponse<InputStream> getFileDelegate(Response<ResponseBody> response) throws CloudException, IOException {
        return this.responseBuilderFactory.<InputStream, CloudException>newInstance(this.serializerAdapter)
                .register(200, new TypeToken<InputStream>() { }.getType())
                .registerError(CloudException.class)
                .build(response);
    }

    public Observable<Void> uploadAsync(String blobName, String blockId, byte[] bytes) {
        return uploadWithServiceResponseAsync(blobName, blockId, bytes).map(new Func1<ServiceResponse<Void>, Void>() {
            @Override
            public Void call(ServiceResponse<Void> response) {
                return response.body();
            }
        });
    }

    public Observable<ServiceResponse<Void>> uploadWithServiceResponseAsync(String blobName, String blockId, byte[] bytes) {
        return service.uploadBlob(blobName, blockId, RequestBody.create(MediaType.parse("application/octet-stream"), bytes))
                .flatMap(new Func1<Response<ResponseBody>, Observable<ServiceResponse<Void>>>() {
                    @Override
                    public Observable<ServiceResponse<Void>> call(Response<ResponseBody> response) {
                        try {
                            ServiceResponse<Void> clientResponse = uploadDelegate(response);
                            return Observable.just(clientResponse);
                        } catch (Throwable t) {
                            return Observable.error(t);
                        }
                    }
                });
    }

    private ServiceResponse<Void> uploadDelegate(Response<ResponseBody> response) throws CloudException, IOException {
        return this.responseBuilderFactory.<Void, CloudException>newInstance(this.serializerAdapter)
                .register(201, new TypeToken<Void>() { }.getType())
                .registerError(CloudException.class)
                .build(response);
    }

    public Observable<Void> commitAsync(String blobName, String blockListXml) {
        return commitWithServiceResponseAsync(blobName, blockListXml).map(new Func1<ServiceResponse<Void>, Void>() {
            @Override
            public Void call(ServiceResponse<Void> response) {
                return response.body();
            }
        });
    }

    public Observable<ServiceResponse<Void>> commitWithServiceResponseAsync(String blobName, String blockListXml) {
        return service.commitBlob(blobName, blockListXml)
                .flatMap(new Func1<Response<ResponseBody>, Observable<ServiceResponse<Void>>>() {
                    @Override
                    public Observable<ServiceResponse<Void>> call(Response<ResponseBody> response) {
                        try {
                            ServiceResponse<Void> clientResponse = commitDelegate(response);
                            return Observable.just(clientResponse);
                        } catch (Throwable t) {
                            return Observable.error(t);
                        }
                    }
                });
    }

    private ServiceResponse<Void> commitDelegate(Response<ResponseBody> response) throws CloudException, IOException {
        return this.responseBuilderFactory.<Void, CloudException>newInstance(this.serializerAdapter)
                .register(201, new TypeToken<Void>() { }.getType())
                .registerError(CloudException.class)
                .build(response);
    }

}