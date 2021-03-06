package code.ponfee.es;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import code.ponfee.commons.json.Jsons;
import code.ponfee.commons.reflect.GenericUtils;
import code.ponfee.commons.util.SpringContextHolder;

/**
 * 测试基类
 * @author Ponfee
 * @param <T>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:test-es.xml" })
public abstract class BaseTest<T> {

    private T bean;

    protected final T getBean() {
        return bean;
    }

    @Before
    public final void setUp() {
        Class<T> type = GenericUtils.getActualTypeArgument(this.getClass());
        if (Object.class != type) {
            bean = SpringContextHolder.getBean(type);
        }
        initialize();
    }

    @After
    public final void tearDown() {
        destroy();
    }

    protected void initialize() {
        // do no thing
    }

    protected void destroy() {
        // do no thing
    }

    public static void consoleJson(Object obj) {
        System.out.println(Jsons.toJson(obj));
    }

    public static void console(Object obj) {
        System.out.println(obj);
    }
}
