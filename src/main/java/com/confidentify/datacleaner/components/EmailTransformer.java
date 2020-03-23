package com.confidentify.datacleaner.components;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Named;

import org.datacleaner.api.Categorized;
import org.datacleaner.api.Configured;
import org.datacleaner.api.InputColumn;
import org.datacleaner.api.InputRow;
import org.datacleaner.api.OutputColumns;
import org.datacleaner.api.StringProperty;
import org.datacleaner.components.categories.ImproveSuperCategory;
import org.datacleaner.util.batch.BatchSink;
import org.datacleaner.util.batch.BatchSource;
import org.datacleaner.util.batch.BatchTransformer;

import com.confidentify.client.ApiClient;
import com.confidentify.client.ApiException;
import com.confidentify.client.ApiResponse;
import com.confidentify.client.api.ProcessApi;
import com.confidentify.client.model.EmailRequest;
import com.confidentify.client.model.EmailRequestRecord;
import com.confidentify.client.model.EmailResponse;
import com.confidentify.client.model.EmailResponseRecord;
import com.confidentify.client.model.ProcessorOutcome;
import com.confidentify.datacleaner.ApiClientProvider;

@Named("Email (conf·ident·ify)")
@Categorized(value = ConfidentifyCategory.class, superCategory = ImproveSuperCategory.class)
public class EmailTransformer extends BatchTransformer {

    private final ApiClientProvider apiClientProvider = ApiClientProvider.get();

    @Configured(order = 10)
    String username;

    @Configured(order = 11)
    @StringProperty(password = true)
    String password;

    @Configured
    InputColumn<String> emailColumn;

    @Override
    public OutputColumns getOutputColumns() {
        final String[] columnNames = { "Email (formatted)", "Email (simplified)", "Email (suggested)", "Verdict", "Info", "Warn" };
        final Class<?>[] columnTypes = { String.class, String.class, String.class, String.class, List.class, List.class };
        return new OutputColumns(columnNames, columnTypes);
    }

    @Override
    public void map(BatchSource<InputRow> source, BatchSink<Object[]> sink) {
        final int count = source.size();

        final List<EmailRequestRecord> requestRecords = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            final InputRow inputRow = source.getInput(i);
            final String inputEmail = inputRow.getValue(emailColumn);
            final String recordId = "r_" + i;
            final EmailRequestRecord requestRecord = new EmailRequestRecord().id(recordId).email(inputEmail);
            requestRecords.add(requestRecord);
        }

        final EmailRequest emailRequest = new EmailRequest();
        emailRequest.setRecords(requestRecords);

        final ApiClient apiClient = apiClientProvider.getApiClient(username, password);
        final ProcessApi api = new ProcessApi(apiClient);
        try {
            final ApiResponse<EmailResponse> response = api.emailPostWithHttpInfo(emailRequest);
            final EmailResponse data = response.getData();
            final List<EmailResponseRecord> records = data.getRecords();
            for (int i = 0; i < count; i++) {
                final EmailResponseRecord responseRecord = records.get(i);
                assert ("r_" + i).equals(responseRecord.getId());
                final ProcessorOutcome outcome = responseRecord.getOutcome();
                final String emailFormatted = responseRecord.getEmailFormatted();
                final String emailSimplified = responseRecord.getEmailSimplified();
                final String emailSuggested = responseRecord.getEmailSuggested();
                final String verdict = outcome == null ? "" : outcome.getVerdict() == null ? "" : outcome.getVerdict().getValue();
                final List<String> info = outcome == null ? null : outcome.getInfo();
                final List<String> warn = outcome == null ? null : outcome.getWarn();
                sink.setOutput(i, new Object[] { emailFormatted, emailSimplified, emailSuggested, verdict, info, warn });
            }
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
    }
}
