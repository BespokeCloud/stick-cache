package com.vercer.cache;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;

public interface Cache<K, V>
{
	void put(K key, V item);
	void put(CacheItem<K, V> item);
	V value(K key);
	Map<K, V> values(Collection<K> keys);
	CacheItem<K, V> item(K key);
	V value(K key, Callable<CacheItem<K, V>> builder);
	Map<K, CacheItem<K, V>> items(Collection<K> keys);
	void invalidate(K key);
	
	class Accessor
	{
		protected void accessed(CacheItem<?, ?> item)
		{
			if (item != null)
			{
				item.accessed = new Date();
				item.accesses++;
			}
		}
	}
}
