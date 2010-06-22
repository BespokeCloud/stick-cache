package com.vercer.engine.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.appengine.api.memcache.Expiration;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.memcache.MemcacheService.SetPolicy;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.vercer.cache.Cache;
import com.vercer.cache.CacheItem;

public class MemcacheCache<K, V> extends Cache.Accessor implements Cache<K, V>
{
	private static final Logger log = Logger.getLogger(MemcacheCache.class.getName());
	private static final Object LOCK = "initialising";
	private static final int LOCK_TIMEOUT_MILLIS = 60000;
	private final int retries;
	private final long retryMillis;

	private final MemcacheService memcache;

	public MemcacheCache()
	{
		this(MemcacheServiceFactory.getMemcacheService());
	}
	
	public MemcacheCache(String namespace)
	{
		this(MemcacheServiceFactory.getMemcacheService(namespace));
	}
	
	public MemcacheCache(MemcacheService memcache)
	{
		this(memcache, 500, 20);
	}

	public MemcacheCache(MemcacheService memcache, long retryMillis, int retries)
	{
		this.memcache = memcache;
		this.retryMillis = retryMillis;
		this.retries = retries;
	}

	public void invalidate(K key)
	{
		memcache.delete(storeKeyValue(key));
	}

	protected Object storeKeyValue(K key)
	{
		return key;
	}

	@SuppressWarnings("unchecked")
	public CacheItem<K, V> item(K key)
	{
		Object value = null;
		try
		{
			value = memcache.get(storeKeyValue(key));
		}
		catch (Exception e)
		{
			log.log(Level.WARNING, "Ignoring exception getting value from memcache", e);
		}
		
		if (value == null || value.equals(LOCK))
		{
			return null;
		}
		else
		{
			CacheItem<K, V> item = (CacheItem<K, V>) value;
			accessed(item);
			return item;
		}
	}

	public void put(K key, V item)
	{
		put(newDefaultCacheItem(key, item));
	}

	protected CacheItem<K, V> newDefaultCacheItem(K key, V item)
	{
		return new CacheItem<K, V>(key, item);
	}

	public void put(CacheItem<K, V> item)
	{
		if (item.getExpirey() != null)
		{
			memcache.put(storeKeyValue(item.getKey()), item, Expiration.onDate(item.getExpirey()));
		}
		else if (item.getDuration() > 0)
		{
			long millis = item.getUnit().toMillis(item.getDuration());
			memcache.put(storeKeyValue(item.getKey()), item, Expiration.byDeltaMillis((int) millis));
		}
		else
		{
			memcache.put(storeKeyValue(item.getKey()), item);
		}
	}

	public V value(K key)
	{
		CacheItem<K, V> item = item(key);
		if (item == null)
		{
			return null;
		}
		else if (item.isValid())
		{
			return item.getValue();
		}
		else
		{
			invalidate(key);
			return null;
		}
	}

	public V value(K key, Callable<CacheItem<K, V>> builder)
	{
		if (memcache.put(storeKeyValue(key), LOCK, Expiration.byDeltaMillis(LOCK_TIMEOUT_MILLIS), SetPolicy.ADD_ONLY_IF_NOT_PRESENT))
		{
			// there was nothing there so we put the lock
			try
			{
				CacheItem<K,V> item = builder.call();
				if (item == null)
				{
					throw new IllegalStateException("Item cannot be null from " + builder);
				}
				memcache.put(storeKeyValue(key), item);
				return item.getValue();
			}
			catch (Exception e)
			{
				// TODO remove a separate lock key
				memcache.delete(storeKeyValue(key)); // remove the lock
				throw new IllegalStateException("Could not put value for key " + key, e);
			}
		}
		else
		{
			// there was already something in the cache for the key
			for (int retry = 0; retry < retries; retry++)
			{
				Object object = null;
				try
				{
					object = memcache.get(storeKeyValue(key));
					if (object == null)
					{
						log.log(Level.SEVERE, "Null item retrieved from memcache after test for existence");
					}
				}
				catch (Exception e)
				{
					log.log(Level.WARNING, "Ignoring exception getting value from memcache", e);
				}
				
				if (object == null)
				{
					// invalid data or item was removed immediately
					return null;
				}
				else if (object.equals(LOCK))
				{
					// cache key is locked so wait
					try
					{
						Thread.sleep(retryMillis);
					}
					catch (InterruptedException e)
					{
						throw new IllegalStateException(e);
					}
				}
				else
				{
					@SuppressWarnings("unchecked")
					CacheItem<K,V> item = (CacheItem<K,V>) object;
					return item.getValue();
				}
			}
			throw new IllegalStateException("Cache timeout");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<K, CacheItem<K, V>> items(Collection<K> keys)
	{
		Map<Object, Object> all = memcache.getAll(Collections2.transform(keys, keyFunction ));
		
		Map<K, CacheItem<K, V>> converted = new HashMap<K, CacheItem<K,V>>(all.size());
		for (K key : keys)
		{
			Object storeKey = storeKeyValue(key);
			Object object = all.get(storeKey);
			if (object != null && !LOCK.equals(object))
			{
				CacheItem<K, V> item = (CacheItem<K, V>) object;
				accessed(item);
				converted.put(key, item);
			}
			// TODO block if find lock?
		}
		
		return converted;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<K, V> values(Collection<K> keys)
	{
		Map<K, CacheItem<K, V>> items = items(keys);
		Map<K, CacheItem<K, V>> filtered = Maps.filterValues(items, validItemFilter );
		return Maps.transformValues(filtered, (Function<CacheItem<K, V>, V>) itemToValueFunction);
	}
	
	private static final Function<?, ?> itemToValueFunction = new Function<CacheItem<Object, Object>, Object>()
	{
		@Override
		public Object apply(CacheItem<Object, Object> from)
		{
			return from.getValue();
		}
	};
	
	private Function<K, Object> keyFunction = new Function<K, Object>()
	{
		@Override
		public Object apply(K from)
		{
			return storeKeyValue(from);
		}
	};
	
	private Predicate<CacheItem<K, V>> validItemFilter = new Predicate<CacheItem<K, V>>()
	{
		@Override
		public boolean apply(CacheItem<K, V> input)
		{
			return input.isValid();
		}
	};
}
