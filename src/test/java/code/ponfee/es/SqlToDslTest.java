package code.ponfee.es;

import org.junit.Test;
import org.nlpcn.es4sql.query.DefaultQueryAction;
import org.nlpcn.es4sql.query.ESActionFactory;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

public class SqlToDslTest {

    @Test
    public void test1() throws Exception {
        String sql = "SELECT case when gender is null then 'aaa'  else gender  end  test , cust_code FROM elasticsearch-sql_test_index";
        String dsl = ESActionFactory.create(NoopClient.get(), sql).explain().explain();
        System.out.println(dsl);
    }

    @Test
    public void test2() throws Exception {
        String sql = "SELECT COUNT(*) AS mycount FROM elasticsearch-sql_test_index/account";
        SqlElasticRequestBuilder srb = ESActionFactory.create(NoopClient.get(), sql).explain();
        System.out.println(srb.explain());
    }

    @Test
    public void test3() throws Exception {
        String sql = "SELECT case when gender is null then 'aaa'  else gender  end  test , cust_code FROM elasticsearch-sql_test_index";
        DefaultQueryAction action = (DefaultQueryAction) ESActionFactory.create(NoopClient.get(), sql);
        System.out.println(action.getRequestBuilder().request().indices());
    }

}
