package com.confidentify.datacleaner.components;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Named;

import org.datacleaner.api.Configured;
import org.datacleaner.api.InputColumn;
import org.datacleaner.api.InputRow;
import org.datacleaner.api.OutputColumns;
import org.datacleaner.api.StringProperty;
import org.datacleaner.util.batch.BatchSink;
import org.datacleaner.util.batch.BatchSource;
import org.datacleaner.util.batch.BatchTransformer;

import com.confidentify.client.ApiClient;
import com.confidentify.client.ApiException;
import com.confidentify.client.ApiResponse;
import com.confidentify.client.api.ProcessApi;
import com.confidentify.client.model.NameRequest;
import com.confidentify.client.model.NameRequestRecord;
import com.confidentify.client.model.NameResponse;
import com.confidentify.client.model.NameResponseRecord;
import com.confidentify.client.model.NameResponseRecordEntity;
import com.confidentify.client.model.ProcessorOutcome;
import com.confidentify.datacleaner.ApiClientProvider;

@Named("Person name (conf·ident·ify)")
public class PersonNameTransformer extends BatchTransformer {

    private final ApiClientProvider apiClientProvider = ApiClientProvider.get();

    @Configured
    String username;

    @Configured
    @StringProperty(password = true)
    String password;

    @Configured
    InputColumn<String> nameColumn;

    @Override
    public OutputColumns getOutputColumns() {
        final String[] columnNames = { "Entities", "Verdict", "Info", "Warn" };
        final Class<?>[] columnTypes = { List.class, String.class, List.class, List.class };
        return new OutputColumns(columnNames, columnTypes);
    }

    @Override
    public void map(BatchSource<InputRow> source, BatchSink<Object[]> sink) {
        final int count = source.size();

        final List<NameRequestRecord> requestRecords = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            final InputRow inputRow = source.getInput(i);
            final String inputName = inputRow.getValue(nameColumn);
            final String recordId = "r_" + i;
            final NameRequestRecord requestRecord = new NameRequestRecord().id(recordId).name(inputName);
            requestRecords.add(requestRecord);
        }

        final NameRequest nameRequest = new NameRequest();
        nameRequest.setRecords(requestRecords);

        final ApiClient apiClient = apiClientProvider.getApiClient(username, password);
        final ProcessApi api = new ProcessApi(apiClient);
        try {
            final ApiResponse<NameResponse> response = api.personNamePostWithHttpInfo(nameRequest);
            final NameResponse data = response.getData();
            final List<NameResponseRecord> records = data.getRecords();
            for (int i = 0; i < count; i++) {
                final NameResponseRecord responseRecord = records.get(i);
                assert ("r_" + i).equals(responseRecord.getId());
                final ProcessorOutcome outcome = responseRecord.getOutcome();
                final List<NameResponseRecordEntity> entities = responseRecord.getEntities();
                final String verdict = outcome.getVerdict() == null ? "" : outcome.getVerdict().getValue();
                final List<String> info = outcome.getInfo();
                final List<String> warn = outcome.getWarn();
                sink.setOutput(i, new Object[] { entities, verdict, info, warn });
            }
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
    }
}
