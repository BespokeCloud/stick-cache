import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.vercer.cache.CompositeCache;
import com.vercer.cache.MemoryCache;
import com.vercer.engine.cache.DatastoreCache;
import com.vercer.engine.cache.MemcacheCache;

public class TestCompositeCache
{
    private final LocalServiceTestHelper helper =
        new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

    @Before
    public void setUp() {
        helper.setUp();
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }
    
	public static class Employee implements Serializable
	{
		public Employee(String name, int age, Date joined)
		{
			this.name = name;
			this.age = age;
			this.joined = joined;
		}
		
		String name;
		int age;
		Date joined;
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void composite()
	{
		MemoryCache<String, Employee> mc = new MemoryCache<String, Employee>(10, false, false); 
		MemcacheCache<String, Employee> mcc = new MemcacheCache<String, Employee>("testspace");
		DatastoreCache<String, Employee> dsc = new DatastoreCache<String, Employee>("testspace");
		
		@SuppressWarnings("unchecked")
		CompositeCache<String, Employee> cc = new CompositeCache<String, Employee>(mc, mcc, dsc);
		
		cc.put("Bob", new Employee("Bob", 25, new Date(15, 8, 106)));
		cc.put("Mary", new Employee("Mary", 32, new Date(17, 12, 109)));
		cc.put("Jim", new Employee("Jim", 74, new Date(25, 9, 100)));
		cc.put("Lucy", new Employee("Lucy", 28, new Date(12, 9, 102)));
		
		// remove bob from memory cache
		mc.invalidate("Bob");
		mcc.invalidate("Jim");
		dsc.invalidate("Mary");
		
		List<String> all = Arrays.asList("Bob", "Jim", "Mary", "Lucy");
		
		// there should be no Bob in memory
		Map<String, Employee> values = mc.values(all);
		Assert.assertEquals(3, values.size());
		Assert.assertNull(values.get("Bob"));

		values = dsc.values(all);
		Assert.assertEquals(3, values.size());
		Assert.assertNull(values.get("Mary"));
		Assert.assertNotNull(values.get("Bob"));
				
		// the composite should return all
		values = cc.values(all);
		Assert.assertEquals(4, values.size());
		
		// make sure we have a valid value
		Assert.assertEquals("Jim", values.get("Jim").name);
		
		// the memory cache should have been updated to include all items
		values = mc.values(all);
		Assert.assertEquals(4, values.size());
	}
}
