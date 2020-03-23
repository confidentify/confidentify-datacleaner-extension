package com.confidentify.datacleaner;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.confidentify.client.ApiClient;
import com.confidentify.client.ApiException;
import com.confidentify.client.api.AuthApi;
import com.confidentify.client.model.AuthRequest;
import com.confidentify.client.model.AuthResponse;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class ApiClientProvider {

    private static final String KEY_SEPARATOR = ":::";
    private static final ApiClientProvider INSTANCE = new ApiClientProvider();

    public static ApiClientProvider get() {
        return INSTANCE;
    }

    private final LoadingCache<String, ApiClient> apiClientCache =
            CacheBuilder.newBuilder().expireAfterWrite(2, TimeUnit.MINUTES).build(new CacheLoader<String, ApiClient>() {
                @Override
                public ApiClient load(String key) throws Exception {
                    final List<String> split = Splitter.on(KEY_SEPARATOR).splitToList(key);
                    assert split.size() == 2;
                    return auth(split.get(0), split.get(1));
                }

            });

    public ApiClient getApiClient(String username, String password) {
        String key = username + KEY_SEPARATOR + password;
        return apiClientCache.getUnchecked(key);
    }

    private ApiClient auth(String username, String password) throws ApiException {
        final ApiClient apiClient = new ApiClient();

        // Authenticate
        final AuthApi authApi = new AuthApi(apiClient);
        final AuthResponse authResponse = authApi.authPost(new AuthRequest().username(username).password(password));
        final String accessToken = authResponse.getAccessToken();
        apiClient.setAccessToken(accessToken);

        return apiClient;
    }
}
