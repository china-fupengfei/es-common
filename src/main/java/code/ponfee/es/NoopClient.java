package code.ponfee.es;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

public class NoopClient extends AbstractClient {

    public NoopClient(ThreadPool threadPool) {
        //new ThreadPool(Settings.builder().build())
        super(Settings.builder().build(), null);
    }

    private static final Client MOCK_CLIENT = new NoopClient(null);

    public static Client get() {
        return MOCK_CLIENT;
    }

    @Override
    public void close() {
        // nothing-todo
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
        Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        // nothing-todo
    }

}
