package com.vercer.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.base.Function;
import com.google.common.collect.Maps;


public class CompositeCache<K, V> implements Cache<K, V>
{
	private Cache<K, V>[] delegates;

	public CompositeCache(Cache<K, V>... delegates)
	{
		this.delegates = delegates;
	}

	public void invalidate(K key)
	{
		for (Cache<K, V> delegate : delegates)
		{
			delegate.invalidate(key);
		}
	}

	public CacheItem<K, V> item(K key)
	{
		for (Cache<K, V> delegate : delegates)
		{
			CacheItem<K,V> item = delegate.item(key);
			if (item != null)
			{
				// add the item to any higher delegates that missed
				for (Cache<K, V> previous : delegates)
				{
					if (previous == delegate) break;
					previous.put(item);
				}
				return item;
			}
		}
		return null;
	}
	
	@Override
	public Map<K, CacheItem<K, V>> items(Collection<K> keys)
	{
		Set<K> remaining = new HashSet<K>(keys);
		Map<K, CacheItem<K, V>> result = null;
		for (Cache<K, V> delegate : delegates)
		{
			// assume a mutable map is returned
			Map<K, CacheItem<K, V>> items = delegate.items(remaining);
			if (result == null)
			{
				result = items;
			}
			else
			{
				result.putAll(items);
			}
				
			// add the items to any higher delegates that missed
			for (Cache<K, V> previous : delegates)
			{
				// do not update current cache or lower
				if (previous == delegate) break;
				
				// TODO implement bulk put
				for (K k : items.keySet())
				{
					CacheItem<K, V> item = items.get(k);
					assert item != null : "Null items should not be returned";
					previous.put(item);
				}
			}
			
			// if we have all our results then finish
			if (result.size() == keys.size())
			{
				return result;
			}
			
			// don't try to get these items again
			remaining.removeAll(items.keySet());
		}
		
		if (result == null)
		{
			return Collections.emptyMap();
		}
		else
		{
			return result;
		}
	}

	public void put(K key, V item)
	{
		for (Cache<K, V> delegate : delegates)
		{
			delegate.put(key, item);
		}
	}

	public void put(CacheItem<K, V> item)
	{
		for (Cache<K, V> delegate : delegates)
		{
			delegate.put(item);
		}
	}

	public V value(K key)
	{
		CacheItem<K, V> item = item(key);
		if(item != null)
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
		else
		{
			return null;
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
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
		V value = value(key);
		if (value == null)
		{
			// use the longest living delegate (e.g. datastore) to lock while building value
			value = delegates[delegates.length - 1].value(key, builder);
			
			// just put the built value in the rest
			for (int i = 0; i < delegates.length - 1; i++)
			{
				delegates[i].put(key, value);
			}
		}
		return value;
	}

}
