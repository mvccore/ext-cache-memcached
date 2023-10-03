<?php

/**
 * MvcCore
 *
 * This source file is subject to the BSD 3 License
 * For the full copyright and license information, please view
 * the LICENSE.md file that are distributed with this source code.
 *
 * @copyright	Copyright (c) 2016 Tom Flidr (https://github.com/mvccore)
 * @license		https://mvccore.github.io/docs/mvccore/5.0.0/LICENSE.md
 */

namespace MvcCore\Ext\Caches;

/**
 * @method static \MvcCore\Ext\Caches\Memcached GetInstance(string|array|NULL $connectionArguments,...)
 * Create or get cached cache wrapper instance.
 * If first argument is string, it's used as connection name.
 * If first argument is array, it's used as connection config array with keys:
 *  - `name`     default: `default`,
 *  - `host`     default: `127.0.0.1`, it could be single IP string 
 *               or an array of IPs and weights for multiple servers:
 *               `['192.168.0.10' => 1, '192.168.0.11' => 2, ...]`,
 *  - `port`     default: `11211`, it could be single port integer 
 *               for single or multiple servers or an array 
 *               of ports for multiple servers: `[11211, 11212, ...]`,
 *  - `database` default: `$_SERVER['SERVER_NAME']`,
 *  - `timeout`  default: `0.5` seconds, only for non-blocking I/O,
 *  - `provider` default: `[...]`, provider specific configuration.
 *  If no argument provided, there is returned `default` 
 *  connection name with default initial configuration values.
 * @method \Memcached|NULL GetProvider() Get `\Memcached` provider instance.
 * @method \MvcCore\Ext\Caches\Memcached SetProvider(\Memcached|NULL $provider) Set `\Memcached` provider instance.
 * @property \Memcached|NULL $provider
 */
class		Memcached
extends		\MvcCore\Ext\Caches\Base
implements	\MvcCore\Ext\ICache {
	
	/**
	 * MvcCore Extension - Cache - Memcached - version:
	 * Comparison by PHP function version_compare();
	 * @see http://php.net/manual/en/function.version-compare.php
	 */
	const VERSION = '5.2.0';

	/** @var array */
	protected static $defaults	= [
		\MvcCore\Ext\ICache::CONNECTION_PERSISTENCE	=> 'default',
		\MvcCore\Ext\ICache::CONNECTION_NAME		=> NULL,
		\MvcCore\Ext\ICache::CONNECTION_HOST		=> '127.0.0.1',
		\MvcCore\Ext\ICache::CONNECTION_PORT		=> 11211,
		\MvcCore\Ext\ICache::CONNECTION_TIMEOUT		=> 0.5, // in seconds, only for non-blocking I/O
		\MvcCore\Ext\ICache::PROVIDER_CONFIG		=> [
			//'\Memcached::OPT_SERIALIZER'			=> '\Memcached::HAVE_IGBINARY', // resolved later in code
			'\Memcached::OPT_LIBKETAMA_COMPATIBLE'	=> TRUE,
			'\Memcached::OPT_POLL_TIMEOUT'			=> 500, // in milliseconds, 0.5s
			'\Memcached::OPT_SEND_TIMEOUT'			=> 1000000, // in microseconds, 0.01s
			'\Memcached::OPT_RECV_TIMEOUT'			=> 1000000, // in microseconds, 0.01s
			'\Memcached::OPT_COMPRESSION'			=> FALSE,
			'\Memcached::OPT_SERVER_FAILURE_LIMIT'	=> 5,
			'\Memcached::OPT_REMOVE_FAILED_SERVERS'	=> TRUE,
		]
	];

	/**
	 * @inheritDoc
	 * @param array $config Connection config array with keys:
	 *  - `name`     default: `default`,
	 *  - `host`     default: `127.0.0.1`, it could be single IP string 
	 *               or an array of IPs and weights for multiple servers:
	 *               `['192.168.0.10' => 1, '192.168.0.11' => 2, ...]`,
	 *  - `port`     default: 11211, it could be single port integer 
	 *               for single or multiple servers or an array 
	 *               of ports for multiple servers: `[11211, 11212, ...]`,
	 *  - `database` default: `$_SERVER['SERVER_NAME']`,
	 *  - `timeout`  default: `0.5` seconds, only for non-blocking I/O,
	 *  - `provider` default: `[...]`, provider specific configuration.
	 */
	protected function __construct (array $config = []) {
		parent::__construct($config);
		$this->installed = class_exists('\Memcached');
	}

	/**
	 * @inheritDoc
	 * @return bool
	 */
	public function Connect () {
		if ($this->connected) {
			return TRUE;
		} else if (!$this->installed) {
			$this->enabled = FALSE;
			$this->connected = FALSE;
		} else {
			$persKey = self::CONNECTION_PERSISTENCE;
			try {
				if (isset($this->config->{$persKey})) {
					$this->provider = new \Memcached($this->config->{$persKey});
				} else {
					$this->provider = new \Memcached();
				}
				if (
					count($this->provider->getServerList()) > 0 && 
					$this->provider->isPersistent()
				) {
					$this->connected = TRUE;
				} else {
					$this->connectConfigure();
					$this->connected = $this->connectExecute();
				}
				$this->enabled = $this->connected;
				if ($this->enabled)
					$this->provider->setOption(
						\Memcached::OPT_PREFIX_KEY, 
						$this->config->{self::CONNECTION_DATABASE}.':'
					);
			} catch (\Exception $e1) { // backward compatibility
				$this->exceptionHandler($e1);
				$this->connected = FALSE;
				$this->enabled = FALSE;
			} catch (\Throwable $e2) {
				$this->exceptionHandler($e2);
				$this->connected = FALSE;
				$this->enabled = FALSE;
			}
		}
		return $this->connected;
	}
	
	/**
	 * Configure connection provider before connection is established.
	 * @return void
	 */
	protected function connectConfigure () {
		// configure provider options:
		$timeoutKey = self::CONNECTION_TIMEOUT;
		$provKey = self::PROVIDER_CONFIG;
		$provConfig = isset($this->config->{$provKey})
			? $this->config->{$provKey}
			: [];
		$provConfigDefault = static::$defaults[$provKey];
		$mcConstBegin = '\Memcached::';
		foreach ($provConfigDefault as $constStr => $rawValue) {
			$const = constant($constStr);
			if (!isset($provConfig[$const])) {
				if (is_string($rawValue) && strpos($rawValue, $mcConstBegin) === 0) {
					if (!defined($rawValue))
						continue;
					$value = constant($rawValue);
				} else {
					$value = $rawValue;
				}
				$provConfig[$const] = $value;
			}
		}
		if (!isset($provConfig[\Memcached::OPT_SERIALIZER]))
			$provConfig[\Memcached::OPT_SERIALIZER] = $this->provider->getOption(\Memcached::HAVE_IGBINARY)
				? \Memcached::SERIALIZER_IGBINARY
				: \Memcached::SERIALIZER_PHP;
		$this->provider->setOption(\Memcached::OPT_CONNECT_TIMEOUT, $this->config->{$timeoutKey} * 1000);
		foreach ($provConfig as $provOptKey => $provOptVal)
			$this->provider->setOption($provOptKey, $provOptVal);
		// configure servers:
		$hosts = [];
		$ports = [];
		$priorities = [];
		$cfgHost = $this->config->{self::CONNECTION_HOST};
		$cfgPort = $this->config->{self::CONNECTION_PORT};
		if (is_string($cfgHost)) {
			$hosts = [$cfgHost];
			$priorities = [1];
		} else if (is_array($cfgHost)) {
			$hosts = array_keys($cfgHost);
			$priorities = array_values($cfgHost);
		}
		$serversCount = count($hosts);
		if (is_int($cfgPort) || is_string($cfgPort)) {
			$ports = array_fill(0, $serversCount, intval($cfgPort));
		} else if (is_array($cfgPort)) {
			$ports = array_map('intval', array_values($cfgPort));
			if (count($ports) !== $serversCount)
				$ports = array_fill(0, $serversCount, $ports[0]);
		}
		foreach ($hosts as $index => $host)
			$this->provider->addServer(
				$host, $ports[$index], $priorities[$index]
			);
	}

	/**
	 * Process every request connection or first persistent connection.
	 * @return bool
	 */
	protected function connectExecute () {
		$toolClass	= $this->application->GetToolClass();
		$version = $toolClass::Invoke(
			[$this->provider, 'getVersion'], [],
			function ($errMsg, $errLevel, $errLine, $errContext) use (& $version) {
				$version = NULL;
			}
		);
		return is_string($version);
	}

	/**
	 * @inheritDoc
	 * @param  string   $key
	 * @param  mixed    $content
	 * @param  int|NULL $expirationSeconds
	 * @param  array    $cacheTags
	 * @return bool
	 */
	public function Save ($key, $content, $expirationSeconds = NULL, $cacheTags = []) {
		$result = FALSE;
		if (!$this->enabled)
			return $result;
		try {
			if ($expirationSeconds === NULL) {
				$this->provider->set($key, $content);
			} else {
				$this->provider->set($key, $content, time() + $expirationSeconds);
			}
			$this->setCacheTags([$key], $cacheTags);
			$result = TRUE;
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  array    $keysAndContents
	 * @param  int|NULL $expirationSeconds
	 * @param  array    $cacheTags
	 * @return bool
	 */
	public function SaveMultiple ($keysAndContents, $expirationSeconds = NULL, $cacheTags = []) {
		$result = FALSE;
		if (!$this->enabled || $keysAndContents === NULL)
			return $result;
		try {
			if ($expirationSeconds === NULL) {
				$this->provider->setMulti($keysAndContents);
			} else {
				$this->provider->setMulti($keysAndContents, time() + $expirationSeconds);
			}
			$this->setCacheTags(array_keys($keysAndContents), $cacheTags);
			$result = TRUE;
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  string        $key
	 * @param  callable|NULL $notFoundCallback function ($cache, $cacheKey) { ... $cache->Save($cacheKey, $data); return $data; }
	 * @return mixed|NULL
	 */
	public function Load ($key, callable $notFoundCallback = NULL) {
		$result = NULL;
		if (!$this->enabled) {
			if ($notFoundCallback !== NULL) {
				try {
					$result = call_user_func_array($notFoundCallback, [$this, $key]);
				} catch (\Exception $e1) { // backward compatibility
					$result = NULL;
					$this->exceptionHandler($e1);
				} catch (\Throwable $e2) {
					$result = NULL;
					$this->exceptionHandler($e2);
				}
			}
			return $result;
		}
		try {
			$rawResult = $this->provider->get($key);
			if ($this->provider->getResultCode() === \Memcached::RES_SUCCESS) {
				$result = $rawResult;
			} else if ($notFoundCallback !== NULL) {
				$result = call_user_func_array($notFoundCallback, [$this, $key]);
			}
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  \string[]     $keys
	 * @param  callable|NULL $notFoundCallback function ($cache, $cacheKey) { ... $cache->Save($cacheKey, $data); return $data; }
	 * @return array|NULL
	 */
	public function LoadMultiple (array $keys, callable $notFoundCallback = NULL) {
		$results = [];
		if (!$this->enabled) {
			if ($notFoundCallback !== NULL) {
				foreach ($keys as $index => $key) {
					try {
						$results[$index] = call_user_func_array(
							$notFoundCallback, [$this, $key]
						);
					} catch (\Exception $e1) { // backward compatibility
						$results[$index] = NULL;
						$this->exceptionHandler($e1);
					} catch (\Throwable $e2) {
						$results[$index] = NULL;
						$this->exceptionHandler($e2);
					}
				}
				return $results;
			} else {
				return NULL;
			}
		}
		try {
			$rawContents = $this->provider->getMulti($keys);
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		foreach ($keys as $index => $key) {
			try {
				if (array_key_exists($key, $rawContents)) {
					$results[$index] = $rawContents[$key];
				} else if ($notFoundCallback !== NULL) {
					$results[$index] = call_user_func_array($notFoundCallback, [$this, $key]);
				}
			} catch (\Exception $e1) { // backward compatibility
				$results[$index] = NULL;
				$this->exceptionHandler($e1);
			} catch (\Throwable $e2) {
				$results[$index] = NULL;
				$this->exceptionHandler($e2);
			}
		}
		return $results;
	}

	/**
	 * @inheritDoc
	 * @param  string $key
	 * @return bool
	 */
	public function Delete ($key) {
		if (!$this->enabled) return FALSE;
		$deleted = FALSE;
		try {
			$deleted = $this->provider->delete($key);
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $deleted;
	}

	/**
	 * DeleteMultiple(['usernames_active_ext'], ['usernames_active_ext' => ['user', 'externals']]);
	 * @inheritDoc
	 * @param  \string[] $keys
	 * @param  array     $keysTags
	 * @return int
	 */
	public function DeleteMultiple (array $keys, array $keysTags = []) {
		if (!$this->enabled) return 0;
		$deletedKeysCount = 0;
		try {
			if (count($keys) > 0) {
				$deletedKeysCount = call_user_func_array(
					[$this->provider, 'deleteMulti'],
					[$keys]
				);
			}
			if (count($keysTags) > 0) {
				$change = FALSE;
				$newTags = [];
				$tags2Remove = [];
				foreach ($keysTags as $cacheKey => $cacheTags) {
					foreach ($cacheTags as $cacheTag) {
						$cacheTagFullKey = self::TAG_PREFIX . $cacheTag;
						$cacheTagKeysSet = $this->provider->get($cacheTagFullKey);
						if ($cacheTagKeysSet !== FALSE) {
							$tagIndex = array_search($cacheKey, $cacheTagKeysSet, TRUE);
							if ($tagIndex !== FALSE) {
								array_splice($cacheTagKeysSet, $tagIndex, 1);
								$change = TRUE;
							}
						}
						if (count($cacheTagKeysSet) > 0) {
							$newTags[$cacheTagFullKey] = $cacheTagKeysSet;
						} else {
							$tags2Remove[] = $cacheTagFullKey;
						}
					}
				}
				if ($change)
					$this->provider->setMulti($newTags);
				if (count($tags2Remove) > 0)
					$this->provider->deleteMulti($tags2Remove);
			}
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $deletedKeysCount;
	}

	/**
	 * @inheritDoc
	 * @param  string|array $tags
	 * @return int
	 */
	public function DeleteByTags ($tags) {
		if (!$this->enabled) return 0;
		$tagsArr = func_get_args();
		if (count($tagsArr) === 1) {
			if (is_array($tags)) {
				$tagsArr = $tags;
			} else if (is_string($tags)) {
				$tagsArr = [$tags];
			}
		}
		$keysToDelete = [];
		foreach ($tagsArr as $tag) {
			$cacheTag = self::TAG_PREFIX . $tag;
			$keysToDelete[$cacheTag] = TRUE;
			$keys2DeleteLocal = $this->provider->get($cacheTag);
			if ($keys2DeleteLocal !== FALSE)
				foreach ($keys2DeleteLocal as $key2DeleteLocal)
					$keysToDelete[$key2DeleteLocal] = TRUE;
		}
		$deletedKeysCount = 0;
		if (count($keysToDelete) > 0) {
			try {
				$deletedKeysCount = call_user_func_array(
					[$this->provider, 'deleteMulti'],
					[array_keys($keysToDelete)]
				);
			} catch (\Exception $e1) { // backward compatibility
				$this->exceptionHandler($e1);
			} catch (\Throwable $e2) {
				$this->exceptionHandler($e2);
			}
		}
		return $deletedKeysCount;
	}

	/**
	 * @inheritDoc
	 * @param  string $key
	 * @return bool
	 */
	public function Has ($key) {
		$result = FALSE;
		if (!$this->enabled) return $result;
		try {
			$this->provider->get($key);
			$result = $this->provider->getResultCode() === \Memcached::RES_SUCCESS;
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  string|\string[] $keys
	 * @return int
	 */
	public function HasMultiple ($keys) {
		$result = 0;
		if (!$this->enabled) return $result;
		$keysArr = func_get_args();
		if (count($keysArr) === 1) {
			if (is_array($keys)) {
				$keysArr = $keys;
			} else if (is_string($keys)) {
				$keysArr = [$keys];
			}
		}
		try {
			$allResults = call_user_func_array(
				[$this->provider, 'getMulti'],
				$keysArr
			);
			if ($allResults !== FALSE)
				$result = count($allResults);
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @return bool
	 */
	public function Clear () {
		$result = FALSE;
		if (!$this->enabled) return $result;
		try {
			$result = $this->provider->flush();
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}
	
	/**
	 * Set up cache tag records if necessary:
	 * @param  \string[] $cacheKeys 
	 * @param  \string[] $cacheTags 
	 * @return \MvcCore\Ext\Caches\Memcached
	 */
	protected function setCacheTags ($cacheKeys = [], $cacheTags = []) {
		if (count($cacheTags) === 0)
			return $this;
		$change = FALSE;
		$newTags = [];
		foreach ($cacheKeys as $cacheKey) {
			foreach ($cacheTags as $cacheTag) {
				$cacheTagFullKey = self::TAG_PREFIX . $cacheTag;
				$cacheTagKeysSet = $this->provider->get($cacheTagFullKey);
				if ($cacheTagKeysSet === FALSE) {
					$cacheTagKeysSet = [$cacheKey];
					$change = TRUE;
				} else if (array_search($cacheKey, $cacheTagKeysSet, TRUE) === FALSE) {
					$cacheTagKeysSet[] = $cacheKey;
					$change = TRUE;
				}
				$newTags[$cacheTagFullKey] = $cacheTagKeysSet;
			}
		}
		if ($change)
			$this->provider->setMulti($newTags);
		return $this;
	}

}