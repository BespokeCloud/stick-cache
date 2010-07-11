package com.vercer.engine.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.vercer.cache.Cache;
import com.vercer.cache.CacheItem;
import com.vercer.engine.persist.util.io.NoDescriptorObjectInputStream;
import com.vercer.engine.persist.util.io.NoDescriptorObjectOutputStream;

public class DatastoreCache<K, V> extends Cache.Accessor implements Cache<K, V>
{
	private final static Logger log = Logger.getLogger(DatastoreCache.class.getName());
	
	private final static String PREFIX = "_cache_";
	private static final String PROPERTY_NAME = "data";
	private static final int BUFFER_SIZE = 10 * 1024;
	private static final String LOCK_PROPERTY_NAME = "lock";
	private static final int RETRIES = 10;
	private static final long RETRY_MILLIS = 5000;

	private final String namespace;
	private final DatastoreService datastore;

	public DatastoreCache()
	{
		this("default");
	}
	public DatastoreCache(String namespace)
	{
		this(DatastoreServiceFactory.getDatastoreService(), namespace);
	}
	
	public DatastoreCache(DatastoreService datastore, String namespace)
	{
		this.datastore = datastore;
		this.namespace = namespace;
	}

	public void invalidate(K key)
	{
		datastore.delete(createDatastoreKey(key));
	}

	public CacheItem<K, V> item(K key)
	{
		try
		{
			Entity entity = datastore.get(createDatastoreKey(key));
			if (entity.hasProperty(LOCK_PROPERTY_NAME))
			{
				return null;
			}
			else
			{
				CacheItem<K, V> item = deserialize((Blob) entity.getProperty(PROPERTY_NAME));
				accessed(item);
				return item;
			}
		}
		catch (EntityNotFoundException e)
		{
			return null;
		}
		catch (Exception e)
		{
			log.log(Level.WARNING, "Ignoring exception deserializing value", e);
			return null;
		}
	}
	
	@Override
	public Map<K, CacheItem<K, V>> items(Collection<K> keys)
	{
		Collection<Key> dskeys = new ArrayList<Key>(keys.size());
		for (K k : keys)
		{
			dskeys.add(createDatastoreKey(k));
		}
		
		Map<Key, Entity> keyToEntity = datastore.get(dskeys);
		Map<K, CacheItem<K, V>> result = new HashMap<K, CacheItem<K, V>>(keyToEntity.size());
		for (K k : keys)
		{
			Key key = createDatastoreKey(k);
			Entity entity = keyToEntity.get(key);
			if (entity != null)
			{
				CacheItem<K, V> item;
				try
				{
					item = deserialize((Blob) entity.getProperty(PROPERTY_NAME));
				}
				catch (Exception e)
				{
					log.log(Level.WARNING, "Ignoring exception deserializing value", e);
					continue;
				}
				accessed(item);
				result.put(k, item);
			}
		}
		return result;
	}

	public void put(K key, V value)
	{
		put(new CacheItem<K, V>(key, value));
	}

	public void put(CacheItem<K, V> item)
	{
		Key key = createDatastoreKey(item.getKey());
 		Entity entity = new Entity(key);
		entity.setProperty(PROPERTY_NAME, serialize(item));
		datastore.put(entity);
	}

	protected Blob serialize(CacheItem<K, V> item)
	{
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream(BUFFER_SIZE);
			NoDescriptorObjectOutputStream ndos = new NoDescriptorObjectOutputStream(baos);
			ndos.writeObject(item);
			return new Blob(baos.toByteArray());
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
	}

	protected CacheItem<K, V> deserialize(Blob blob) throws IOException, ClassNotFoundException
	{
		ByteArrayInputStream bais = new ByteArrayInputStream(blob.getBytes());
		NoDescriptorObjectInputStream ndis = new NoDescriptorObjectInputStream(bais);
		@SuppressWarnings("unchecked")
		CacheItem<K, V> item = (CacheItem<K, V>) ndis.readObject();
		return item;
	}

	public V value(K key)
	{
		CacheItem<K, V> item = item(key);
		if (item == null)
		{
			return null;
		}
		else
		{
			if (item.isValid())
			{
				return item.getValue();
			}
			else
			{
				invalidate(key);
				return null;
			}
		}
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public Map<K, V> values(Collection<K> keys)
	{
		return Maps.transformValues(items(keys), (Function<CacheItem<K, V>, V>) itemToValueFunction);
	}
	
	private static final Function<?, ?> itemToValueFunction = new Function<CacheItem<Object, Object>, Object>()
	{
		@Override
		public Object apply(CacheItem<Object, Object> from)
		{
			return from.getValue();
		}
	};

	public V value(K key, Callable<CacheItem<K, V>> builder)
	{
		Key dskey = createDatastoreKey(key);
		try
		{
			for (int i = 0; i < RETRIES; i++)
			{
				Entity entity = datastore.get(dskey);

				if (entity.hasProperty(LOCK_PROPERTY_NAME))
				{
					Thread.sleep(RETRY_MILLIS);
				}
				else
				{
					Blob blob = (Blob) entity.getProperty(PROPERTY_NAME);
					CacheItem<K,V> item = deserialize(blob);
					if (item.isValid())
					{
						return item.getValue();
					}
				}
			}
			throw new IllegalStateException("Datastore lock timed out");
		}
		catch (EntityNotFoundException e)
		{
			// fall through to create item
		}
		catch (Exception e)
		{
			log.log(Level.WARNING, "Ignoring exception deserializing value", e);
		}

		// no item was found or it was expired
		// TODO look at doing this in a transaction i.e. separate lock entity

		// lock the datastore so others don't also create item
		Entity lockEntity = new Entity(dskey);
		lockEntity.setProperty(LOCK_PROPERTY_NAME, Boolean.TRUE);
		datastore.put(lockEntity);

		CacheItem<K,V> item;
		try
		{
			item = builder.call();
			put(item);
		}
		catch (Exception e)
		{
			datastore.delete(dskey); // remove the lock entity
			throw new IllegalStateException(e);
		}
		return item.getValue();
	}

	private Key createDatastoreKey(K value)
	{
		return KeyFactory.createKey(PREFIX + namespace, keyName(value));
	}

	protected String keyName(K value)
	{
		return value.toString();
	}

}
