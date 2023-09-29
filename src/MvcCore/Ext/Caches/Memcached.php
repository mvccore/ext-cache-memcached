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

class Memcached implements \MvcCore\Ext\ICache {
	
	/**
	 * MvcCore Extension - Cache - Memcached - version:
	 * Comparison by PHP function version_compare();
	 * @see http://php.net/manual/en/function.version-compare.php
	 */
	const VERSION = '5.0.1';

	/** @var array */
	protected static $instances = [];

	/** @var array */
	protected static $defaults = [
		\MvcCore\Ext\ICache::CONNECTION_NAME		=> 'default',
		\MvcCore\Ext\ICache::CONNECTION_HOST		=> '127.0.0.1',
		\MvcCore\Ext\ICache::CONNECTION_PORT		=> 6379,
		\MvcCore\Ext\ICache::CONNECTION_DATABASE	=> NULL,
		\MvcCore\Ext\ICache::CONNECTION_TIMEOUT		=> NULL,
	];

	/** @var \stdClass|NULL */
	protected $config = NULL;

	/** @var \Memcached|NULL */
	protected $redis = NULL;

	/** @var bool */
	protected $enabled = FALSE;

	/** @var bool|NULL */
	protected $connected = NULL;

	/** @var \MvcCore\Application */
	protected $application = TRUE;

	/**
	 * @inheritDoc
	 * @param string|array|NULL $connectionArguments...
	 * If string, it's used as connection name.
	 * If array, it's used as connection config array with keys:
	 *  - `name`		default: 'default'
	 *  - `host`		default: '127.0.0.1'
	 *  - `port`		default: 6379
	 *  - `database`	default: $_SERVER['SERVER_NAME']
	 *  - `timeout`		default: NULL
	 *  If NULL, there is returned `default` connection
	 *  name with default initial configuration values.
	 * @return \MvcCore\Ext\Caches\Redis
	 */
	public static function GetInstance (/*...$connectionNameOrArguments = NULL*/) {
		$args = func_get_args();
		$nameKey = self::CONNECTION_NAME;
		$config = static::$defaults;
		$connectionName = $config[$nameKey];
		if (isset($args[0])) {
			$arg = & $args[0];
			if (is_string($arg)) {
				$connectionName = $arg;
			} else if (is_array($arg)) {
				$connectionName = isset($arg[$nameKey])
					? $arg[$nameKey]
					: static::$defaults[$nameKey];
				$config = $arg;
			} else if ($arg !== NULL) {
				throw new \InvalidArgumentException(
					"[".get_class()."] Cache instance getter argument could be ".
					"only a string connection name or connection config array."
				);
			}
		}
		if (!isset(self::$instances[$connectionName]))
			self::$instances[$connectionName] = new static($config);
		return self::$instances[$connectionName];
	}

	/**
	 * @inheritDoc
	 * @param array $config Connection config array with keys:
	 *  - `name`		default: 'default'
	 *  - `host`		default: '127.0.0.1'
	 *  - `port`		default: 6379
	 *  - `database`	default: $_SERVER['SERVER_NAME']
	 *  - `timeout`		default: NULL
	 */
	protected function __construct (array $config = []) {
		$hostKey	= self::CONNECTION_HOST;
		$portKey	= self::CONNECTION_PORT;
		$timeoutKey	= self::CONNECTION_TIMEOUT;
		$dbKey		= self::CONNECTION_DATABASE;

		if (!isset($config[$hostKey]))
			$config[$hostKey] = static::$defaults[$hostKey];
		if (!isset($config[$portKey]))
			$config[$portKey] = static::$defaults[$portKey];
		if (
			!isset($config[$timeoutKey]) && 
			static::$defaults[$timeoutKey] !== NULL
		) 
			$config[$timeoutKey] = static::$defaults[$timeoutKey];
		if (!isset($config[$dbKey]))
			$config[$dbKey]	= static::$defaults[$dbKey];

		$this->config = (object) $config;
		$this->application = \MvcCore\Application::GetInstance();
	}

	/**
	 * @inheritDoc
	 * @return bool
	 */
	public function Connect () {
		if (!class_exists('\Redis')) {
			$this->enabled = FALSE;
			$this->connected = FALSE;
		} else {
			$toolClass = $this->application->GetToolClass();
			$debugClass = $this->application->GetDebugClass();
			$timeoutKey = self::CONNECTION_TIMEOUT;

			$connectionArguments = [
				$this->config->{self::CONNECTION_HOST},
				$this->config->{self::CONNECTION_PORT}
			];
			if (isset($this->config->{$timeoutKey})) {
				$connectionArguments[] = $this->config->{$timeoutKey};
			} else if (static::$defaults[$timeoutKey] !== NULL) {
				$connectionArguments[] = static::$defaults[$timeoutKey];
			}

			try {
				$this->redis = new \Redis();
				$connected = $toolClass::Invoke(
					[$this->redis, 'connect'],
					$connectionArguments,
					function ($errMsg, $errLevel, $errLine, $errContext) use (& $connected) {
						$connected = FALSE;
					}
				);
				$this->connected = !!$connected;
				$this->enabled = $this->connected;
				if ($this->enabled)
					$this->redis->setOption(
						\Redis::OPT_PREFIX, 
						$this->config->{self::CONNECTION_DATABASE}.':'
					);

			} catch (\Throwable $e) {
				$debugClass::Log($e);
				$this->connected = FALSE;
				$this->enabled = FALSE;
			}
		}
		return $this->connected;
	}

	/**
	 * @inheritDoc
	 * @return \Redis|NULL
	 */
	public function GetResource () {
		return $this->redis;
	}

	/**
	 * @inheritDoc
	 * @param  \Redis $resource
	 * @return \MvcCore\Ext\Caches\Redis
	 */
	public function SetResource ($resource) {
		$this->redis = $resource;
		return $this;
	}

	/**
	 * @inheritDoc
	 * @return \stdClass
	 */
	public function GetConfig () {
		return $this->config;
	}

	/**
	 * Enable/disable cache component.
	 * @param  bool $enable
	 * @return \MvcCore\Ext\Caches\Redis
	 */
	public function SetEnabled ($enabled) {
		if ($enabled) {
			$enabled = (class_exists('\Redis') && (
				$this->connected === NULL ||
				$this->connected === TRUE
			));
		}
		$this->enabled = $enabled;
		return $this;
	}

	/**
	 * @inheritDoc
	 * @return bool
	 */
	public function GetEnabled () {
		return $this->enabled;
	}

	/**
	 * @inheritDoc
	 * @param  array $ops Keys are redis functions names, values are functions arguments.
	 * @return array
	 */
	public function ProcessTransaction (array $ops = []) {
		$result = [];
		try {
			$multiRedis = $this->redis->multi();
			foreach ($ops as $oppName => $args)
				$multiRedis = $multiRedis->{$oppName}($args);
			$result = $multiRedis->exec();
		} catch (\Throwable $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
		}
		return $result;
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
				$this->redis->set($key, serialize($content));
			} else {
				$this->redis->setEx($key, $expirationSeconds, serialize($content));
			}
			if ($cacheTags)
				foreach ($cacheTags as $tag)
					$this->redis->sAdd(self::TAG_PREFIX . $tag, $key);
			$result = TRUE;
		} catch (\Throwable $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
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
			$keysAndContents = array_map('serialize', $keysAndContents);
			if ($expirationSeconds === NULL) {
				$this->redis->mSet($keysAndContents);
			} else {
				foreach ($keysAndContents as $key => $serializedContent)
					$this->redis->setEx($key, $expirationSeconds, $serializedContent);
			}
			if ($cacheTags) {
				$args = array_keys($keysAndContents);
				if ($args !== NULL) {
					array_unshift($args, '');
					foreach ($cacheTags as $tag) {
						$args[0] = self::TAG_PREFIX . $tag;
						call_user_func_array([$this->redis, 'sAdd'], $args);
					}
				}
			}
			$result = TRUE;
		} catch (\Throwable $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
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
		$debugClass = $this->application->GetDebugClass();
		if (!$this->enabled) {
			if ($notFoundCallback !== NULL) {
				try {
					$result = call_user_func_array($notFoundCallback, [$this, $key]);
				} catch (\Exception $e1) { // backward compatibility
					if ($this->application->GetEnvironment()->IsDevelopment()) {
						throw $e1;
					} else {
						$debugClass::Log($e1);
						$result = NULL;
					}
				} catch (\Throwable $e1) {
					if ($this->application->GetEnvironment()->IsDevelopment()) {
						throw $e1;
					} else {
						$debugClass::Log($e1);
						$result = NULL;
					}
				}
			}
			return $result;
		}
		try {
			$rawContent = $this->redis->get($key);
			if ($rawContent !== FALSE) {
				$result = unserialize($rawContent);
			} else if ($notFoundCallback !== NULL) {
				$result = call_user_func_array($notFoundCallback, [$this, $key]);
			}
		} catch (\Exception $e2) { // backward compatibility
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e2;
			} else {
				$debugClass::Log($e2);
				$result = NULL;
			}
		} catch (\Throwable $e2) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e2;
			} else {
				$debugClass::Log($e2);
				$result = NULL;
			}
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  \string[]     $keys
	 * @param  callable|NULL $notFoundCallback function ($cache, $cacheKey) { ... $cache->Save($cacheKey, $data); return $data; }
	 * @return mixed|NULL
	 */
	public function LoadMultiple (array $keys, callable $notFoundCallback = NULL) {
		$results = [];
		$keysArr = func_get_args();
		if (count($keysArr) === 1) {
			if (is_array($keys)) {
				$keysArr = $keys;
			} else if (is_string($keys)) {
				$keysArr = [$keys];
			}
		}
		$debugClass = $this->application->GetDebugClass();
		if (!$this->enabled) {
			if ($notFoundCallback !== NULL) {
				foreach ($keysArr as $index => $key) {
					try {
						$results[$index] = call_user_func_array(
							$notFoundCallback, [$this, $key]
						);
					} catch (\Exception $e1) { // backward compatibility
						if ($this->application->GetEnvironment()->IsDevelopment()) {
							throw $e1;
						} else {
							$debugClass::Log($e1);
							$results[$index] = NULL;
						}
					} catch (\Throwable $e1) {
						if ($this->application->GetEnvironment()->IsDevelopment()) {
							throw $e1;
						} else {
							$debugClass::Log($e1);
							$results[$index] = NULL;
						}
					}
				}
				return $results;
			} else {
				return NULL;
			}
		}
		try {
			$rawContents = $this->redis->mGet($keysArr);
		} catch (\Exception $e2) { // backward compatibility
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e2;
			} else {
				$debugClass::Log($e2);
			}
		} catch (\Throwable $e2) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e2;
			} else {
				$debugClass::Log($e2);
			}
		}
		foreach ($rawContents as $index => $rawContent) {
			try {
				if ($rawContent !== FALSE) {
					$results[$index] = unserialize($rawContent);
				} else if ($notFoundCallback !== NULL) {
					$results[$index] = call_user_func_array($notFoundCallback, [$this, $keys[$index]]);
				}
			} catch (\Exception $e2) { // backward compatibility
				if ($this->application->GetEnvironment()->IsDevelopment()) {
					throw $e3;
				} else {
					$debugClass::Log($e3);
					$results[$index] = NULL;
				}
			} catch (\Throwable $e3) {
				if ($this->application->GetEnvironment()->IsDevelopment()) {
					throw $e3;
				} else {
					$debugClass::Log($e3);
					$results[$index] = NULL;
				}
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
		$deletedKeysCount = 0;
		try {
			$deletedKeysCount = $this->redis->del($key);
		} catch (\Throwable $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
		}
		return $deletedKeysCount;
	}

	/**
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
					[$this->redis, 'del'],
					$keys
				);
			}
			if (count($keysTags) > 0) {
				$setsAndKeysToRemove = [];
				foreach ($keysTags as $key => $tags) {
					foreach ($tags as $tag) {
						$cacheTag = self::TAG_PREFIX . $tag;
						if (!isset($setsAndKeysToRemove[$cacheTag]))
							$setsAndKeysToRemove[$cacheTag] = [];
						$setsAndKeysToRemove[$cacheTag][] = $key;
					}
				}
				foreach ($setsAndKeysToRemove as $cacheTag => $keysToRemove) {
					if ($keysToRemove === NULL) continue;
					array_unshift($keysToRemove, $cacheTag);
					call_user_func_array(
						[$this->redis, 'sRem'],
						$keysToRemove
					);
				}
			}
		} catch (\Throwable $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
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
			$keysToDelete[] = $cacheTag;
			$keysToDeleteLocal = $this->redis->sMembers($cacheTag);
			$keysToDelete = array_merge($keysToDelete, $keysToDeleteLocal);
		}
		$deletedKeysCount = 0;
		if (count($keysToDelete) > 0) {
			try {
				$deletedKeysCount = call_user_func_array(
					[$this->redis, 'del'],
					$keysToDelete
				);
			} catch (\Throwable $e) {
				if ($this->application->GetEnvironment()->IsDevelopment()) {
					throw $e;
				} else {
					$debugClass = $this->application->GetDebugClass();
					$debugClass::Log($e);
				}
			}
		}
		return $deletedKeysCount;
	}

	/**
	 * @inheritDoc
	 * @param  string $key
	 * @return int
	 */
	public function Has ($key) {
		$result = 0;
		if (!$this->enabled) return $result;
		try {
			$result = $this->redis->exists($key);
		} catch (\Throwable $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  \string[] $key
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
			$result = call_user_func_array(
				[$this->redis, 'exists'],
				$keysArr
			);
		} catch (\Throwable $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
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
			$result = $this->redis->flushDb();
		} catch (\Throwable $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
		}
		return $result;
	}
}