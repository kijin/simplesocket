<?php

/**
 * Redis Client Library for PHP 5.2+
 * 
 * Based on Simple Socket Client, this library implements the text protocol
 * used by the Redis server. Command specifications are based on Redis 1.2,
 * but earlier versions of Redis are also fully supported.
 * 
 * Supported Features:
 *   - Automatic detection of Redis version.
 *   - Multi-bulk commands. (Only available with Redis 1.1+)
 *   - Support for possible new commands using the multi-bulk format.
 * 
 * Unsupported Features:
 *   - Monitor command.
 *   - Key distribution and load balancing.
 * 
 * If you attempt to use a command that is not supported by your version of
 * Redis, you will get an Exception. The same is true for most server error
 * conditions; it is your responsibility to catch those exceptions.
 * 
 * If you want to distribute keys across several Redis instances, use a more
 * fully featured client library (there are quite a few out there, you know);
 * or use this library in combination with your own key distribution algorithm.
 * May the author suggests Distrib (http://github.com/kijin/distrib).
 * 
 * URL: http://github.com/kijin/simplesocket
 * Version: 0.1.2
 */

require_once(dirname(__FILE__) . '/../simplesocketclient.php');

/**
 * Copyright (c) 2010, Kijin Sung <kijinbear@gmail.com>
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

class RedisClient extends SimpleSocketClient
{
    // Configuration.
    
    private $compression = false;
    private $redis_version = false;
    
    
    // Set compression threshold.
    
    public function set_compression($threshold = 1024)
    {
        // Save to instance, or false if an invalid value has been given.
        
        $this->compression = (int)$threshold ? (int)$threshold : false;
    }
    
    
    // Set Redis version. This method will disable automatic checking.
    
    public function set_redis_version($version)
    {
        // Save to instance.
        
        $this->redis_version = (float)$version;
    }
    
    
    // Get Redis version. This method will automatically check the server.
    
    public function get_redis_version()
    {
        // Use an INFO command to obtain the version.
        
        if ($this->redis_version === false) 
        {
            $this->write('INFO');
            $info = $this->get_response();
            $this->redis_version = (float)substr($info, 14, 3);
        }
        
        // Return the cached value.
        
        return $this->redis_version;
    }
    
    
    // AUTH method.
    
    public function auth($password)
    {
        // Expect: status.
        
        $this->validate_key($password);
        $this->write('AUTH ' . $password);
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // SELECT method.
    
    public function select($db)
    {
        // Expect: status.
        
        $this->validate_key($db);
        $this->write('SELECT ' . $db);
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // DBSIZE method.
    
    public function dbsize()
    {
        // Expect: integer.
        
        $this->write('DBSIZE');
        return $this->get_response();
    }
    
    
    // EXISTS method.
    
    public function exists($key)
    {
        // Expect: integer (cast to bool).
        
        $this->validate_key($key);
        $this->write('EXISTS ' . $key);
        return (bool)$this->get_response();
    }
    
    
    // TYPE method.
    
    public function type($key)
    {
        // Expect: single line.
        
        $this->validate_key($key);
        $this->write('TYPE ' . $key);
        return strtolower($this->get_response());
    }
    
    
    // KEYS method.
    
    public function keys($pattern)
    {
        // Expect: bulk (convert to array).
        
        $this->validate_key($pattern);
        $this->write('KEYS ' . $pattern);
        return explode(' ', (string)$this->get_response());
    }
    
    
    // RANDOMKEY method.
    
    public function randomkey()
    {
        // Expect: single line.
        
        $this->write('RANDOMKEY');
        return (string)$this->get_response();
    }
    
    
    // GET method.
    
    public function get($key)
    {
        // If the key is an array, pass on to getmulti().
        
        if (is_array($key)) return $this->getmulti($key);

        // Expect: bulk.
        
        $this->validate_key($key);
        $this->write('GET ' . $key);
        return $this->decode($this->get_response());
    }
    
    
    // MGET method.
    
    public function mget($keys)
    {
        // Validate the keys.
        
        foreach ($keys as $key) $this->validate_key($key);
        
        // If no keys are supplied, return an empty array.
        
        $count = count($keys);
        if (!$count) return array();
        
        // Expect: multi-bulk.
        
        $this->write('MGET ' . implode(' ', $keys));
        $response = $this->get_response();
        if (!is_array($response) || count($response) !== $count) return false;
        
        // Convert to an associative array.
        
        $return = array();
        $count = count($response);
        for ($i = 0; $i < $count; $i ++)
        {
            $return[$keys[$i]] = $this->decode($response[$i]);
        }
        return $return;
    }
    
    
    // GETSET method.
    
    public function getset($key, $value)
    {
        // Validate the key.
        
        $this->validate_key($key);
        
        // Attempt a multi-bulk command. (Expect: bulk)
        
        $this->multi_bulk_command(array('GETSET', $key, $this->encode($value)), 2);
        return $this->decode($this->get_response());
    }
    
    
    // MOVE method.
    
    public function move($key, $target_db)
    {
        // Expect: integer (cast to bool).
        
        $this->validate_key($key);
        $this->validate_key($target_db);
        $this->write('MOVE ' . $key . ' ' . $target_db);
        return (bool)$this->get_response();
    }
    
    
    // RENAME method.
    
    public function rename($oldkey, $newkey, $overwrite = true)
    {
        // Validate the keys.
        
        $this->validate_key($oldkey);
        $this->validate_key($newkey);
        
        // Write either RENAME or RENAMENX.
        
        $command = $overwrite ? 'RENAME' : 'RENAMENX';
        $command = $this->build_command($command, $oldkey, $newkey);
        $this->write($command);
        
        // Expect: status or integer.
        
        $response = $this->get_response();
        return ($response === true || $response === 1) ? true : false;
    }
    
    
    // RENAMENX method.
    
    public function renamenx($oldkey, $newkey)
    {
        // Call rename() with $overwrite = false.
        
        return $this->rename($oldkey, $newkey, false);        
    }
    
    
    // SET method.
    
    public function set($key, $value, $overwrite = true)
    {
        // Validate the key.
        
        $this->validate_key($key);
        
        // Serialize and/or compress the value.
        
        $value = $this->encode($value);
        
        // Write either SET or SETNX.
        
        $command = $overwrite ? 'SET' : 'SETNX';
        $command = $this->build_command($command, $key, strlen($value));
        $this->write($command . "\r\n" . $value . "\r\n", false);
        
        // Expect: status or integer.
        
        $response = $this->get_response();
        return ($response === true || $response === 1) ? true : false;
    }
    
    
    // SETNX method.
    
    public function setnx($key, $value)
    {
        // Call set() with $overwrite = false.
        
        return $this->set($key, $value, false);
    }
    
    
    // MSET method. (Redis 1.1+)
    
    public function mset($pairs, $overwrite = true)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.1)
        {
            throw new Exception('MSET is only supported in Redis 1.1+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Validate the keys.
        
        $keys = array_keys($pairs);
        foreach ($keys as $key) $this->validate_key($key);
        
        // Assemble the arguments for an MSET[NX] command.
        
        $args = array($overwrite ? 'MSET' : 'MSETNX');
        foreach ($pairs as $key => $value)
        {
            $args[] = (string)$key;
            $args[] = $this->encode($value);
        }
        
        // Attempt a multi-bulk command. (Expect: integer; cast to bool)
        
        $mbc = $this->multi_bulk_command($args);
        if ($mbc) return (bool)$this->get_response();
        
        // If MBC is not available, return false.
        
        return false;
    }
    
    
    // MSETNX method. (Redis 1.1+)
    
    public function msetnx($pairs)
    {
        // Call mset() with $overwrite = false.
        
        return $this->mset($pairs, false);
    }    
    
    
    // EXPIRE method.
    
    public function expire($key, $expiry)
    {
        // If $expiry >= 30 days, treat it as a timestamp.
        
        $timestamp = ($expiry >= 2592000) ? true : false;
        
        // Expect: status.
        
        $this->validate_key($key);
        $command = $timestamp ? 'EXPIREAT' : 'EXPIRE';
        $this->write($command . ' ' . $key . ' ' . $expiry);
        return $this->get_response();
    }
    
    
    // TTL method.
    
    public function ttl($key)
    {
        // Expect: integer.
        
        $this->validate_key($key);
        $this->write('TTL ' . $key);
        return $this->get_response();
    }
    
    
    // DEL method.
    
    public function del($key)
    {
        // Validate the key, or array of keys.
        
        if (is_array($key))
        {
            foreach ($key as $k) $this->validate_key($k);
            $key = implode(' ', $key);
        }
        else
        {
            $this->validate_key($key);            
        }     
        
        // Expect: integer.
        
        $this->write('DEL ' . $key);
        return $this->get_response();
    }
    
    
    // DELETE method, an alias to del().
    
    public function delete($key)
    {
        // Call the real method.
        
        return $this->del($key);
    }
    
    
    // INCR method.
    
    public function incr($key, $diff = 1)
    {
        // Expect: integer.
        
        $this->validate_key($key);
        $this->write('INCRBY ' . $key . ' ' . (int)$diff);
        return $this->get_response();
    }
    
    
    // DECR method.
    
    public function decr($key, $diff = 1)
    {
        // Expect: integer.
        
        $this->validate_key($key);
        $this->write('DECRBY ' . $key . ' ' . (int)$diff);
        return $this->get_response();
    }
    
    
    // LPUSH method.
    
    public function lpush($key, $value)
    {
        // Expect: status.
        
        $this->validate_key($key);
        $this->multi_bulk_command(array('LPUSH', $key, $this->encode($value)), 2);
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // RPUSH method.
    
    public function rpush($key, $value)
    {
        // Expect: status.
        
        $this->validate_key($key);
        $this->multi_bulk_command(array('RPUSH', $key, $this->encode($value)), 2);
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // LLEN method.
    
    public function llen($key)
    {
        // Expect: integer.
        
        $this->validate_key($key);
        $this->write('LLEN ' . $key);
        return $this->get_response();
    }
    
    
    // LRANGE method.
    
    public function lrange($key, $start, $end)
    {
        // Expect: multi-bulk.
        
        $this->validate_key($key);
        $command = $this->build_command('LRANGE', $key, (int)$start, (int)$end);
        $this->write($command);
        return $this->decode($this->get_response());
    }
    
    
    // LTRIM method.
    
    public function ltrim($key, $start, $end)
    {
        // Expect: status.
        
        $this->validate_key($key);
        $command = $this->build_command('LTRIM', $key, (int)$start, (int)$end);
        $this->write($command);
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // LINDEX method.
    
    public function lindex($key, $index)
    {
        // Expect: bulk.
        
        $this->validate_key($key);
        $command = $this->build_command('LINDEX', $key, (int)$index);
        $this->write($command);
        return $this->decode($this->get_response());
    }
    
    
    // LSET method.
    
    public function lset($key, $index, $value)
    {
        // Expect: status.
        
        $this->validate_key($key);
        $this->multi_bulk_command(array('LSET', $key, $index, $this->encode($value)), 3);
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // LREM method.
    
    public function lrem($key, $count, $value)
    {
        // Expect: integer.
        
        $this->validate_key($key);
        $this->multi_bulk_command(array('LREM', $key, $count, $this->encode($value)), 3);
        return $this->get_response();
    }
    
    
    // LPOP method.
    
    public function lpop($key)
    {
        // Expect: bulk.
        
        $this->validate_key($key);
        $this->write('LPOP ' . $key);
        return $this->decode($this->get_response());
    }
    
    
    // RPOP method.
    
    public function rpop($key)
    {
        // Expect: bulk.
        
        $this->validate_key($key);
        $this->write('RPOP ' . $key);
        return $this->decode($this->get_response());
    }
    
    
    // BLPOP method. (Redis 1.3+)
    
    public function blpop($keys, $timeout)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.3)
        {
            throw new Exception('MSET is only supported in Redis 1.3+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Take care of arrays.
        
        if (is_array($keys))
        {
            foreach ($keys as $key) $this->validate_key($key);
            array_unshift($keys, 'BLPOP');
            $keys[] = (int)$timeout;
        }
        else
        {
            $this->validate_key($keys);    
            $keys = array('BLPOP', $keys, (int)$timeout);
        }
        
        // Expect: multi-bulk.
        
        $this->multi_bulk_command($keys);        
        return $this->decode($this->get_response());
    }
    
    
    // BRPOP method. (Redis 1.3+)
    
    public function brpop($keys, $timeout)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.3)
        {
            throw new Exception('MSET is only supported in Redis 1.3+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Take care of arrays.
        
        if (is_array($keys))
        {
            foreach ($keys as $key) $this->validate_key($key);
            array_unshift($keys, 'BRPOP');
            $keys[] = (int)$timeout;
        }
        else
        {
            $this->validate_key($keys);    
            $keys = array('BRPOP', $keys, (int)$timeout);
        }
        
        // Expect: multi-bulk.
        
        $this->multi_bulk_command($keys);        
        return $this->decode($this->get_response());
    }
    
    
    // RPOPLPUSH method. (Redis 1.1+)
    
    public function rpoplpush($source_key, $destination_key)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.1)
        {
            throw new Exception('MSET is only supported in Redis 1.1+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Expect: bulk.
        
        $this->validate_key($source_key);
        $this->validate_key($destination_key);
        $mbc = $this->multi_bulk_command(array('RPOPLPUSH', $source_key, $destination_key));
        return $mbc ? $this->decode($this->get_response()) : false;
    }
    
    
    // SADD method.
    
    public function sadd($key, $member)
    {
        // Expect: integer (cast to bool).
        
        $this->validate_key($key);
        $this->multi_bulk_command(array('SADD', $key, $this->encode($member)), 2);
        return (bool)$this->get_response();
    }
    
    
    // SREM method.
    
    public function srem($key, $member)
    {
        // Expect: integer (cast to bool).
        
        $this->validate_key($key);
        $this->multi_bulk_command(array('SREM', $key, $this->encode($member)), 2);
        return (bool)$this->get_response();
    }
    
    
    // SPOP method.
    
    public function spop($key)
    {
        // Expect: bulk.
        
        $this->validate_key($key);
        $this->write('SPOP ' . $key);
        return $this->decode($this->get_response());
    }
    
    
    // SMOVE method.
    
    public function smove($source_key, $destination_key, $member)
    {
        // Expect: integer (cast to bool).
        
        $this->validate_key($source_key);
        $this->validate_key($destination_key);
        $this->multi_bulk_command(array('SMOVE', $source_key, $destination_key, $this->encode($member)), 3);
        return (bool)$this->get_response();
    }
    
    
    // SCARD method.
    
    public function scard($key)
    {
        // Expect: integer.
        
        $this->validate_key($key);
        $this->write('SCARD ' . $key);
        return $this->get_response();
    }
    
    
    // SISMEMBER method.
    
    public function sismember($key, $member)
    {
        // Expect: integer (cast to bool).
        
        $this->validate_key($key);
        $this->multi_bulk_command(array('SISMEMBER', $key, $this->encode($member)), 2);
        return (bool)$this->get_response();
    }
    
    
    // SINTER method.
    
    public function sinter( /* keys */ )
    {
        // Flatten the arguments.
        
        $args = func_get_args();
        $keys = $this->array_flatten($args);
        
        // Expect: multi-bulk.
        
        $this->write('SINTER ' . implode(' ', $keys));
        return $this->decode($this->get_response());
    }
    
    
    // SINTERSTORE method.
    
    public function sinterstore($destination /* keys */ )
    {
        // Flatten the arguments.
        
        $this->validate_key($destination);
        $args = func_get_args(); array_shift($args);
        $keys = $this->array_flatten($args);
        
        // Expect: status.
        
        $this->write('SINTERSTORE ' . $destination . ' ' . implode(' ', $keys));
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // SUNION method.
    
    public function sunion( /* keys */ )
    {
        // Flatten the arguments.
        
        $args = func_get_args();
        $keys = $this->array_flatten($args);
        
        // Expect: multi-bulk.
        
        $this->write('SUNION ' . implode(' ', $keys));
        return $this->decode($this->get_response());
    }
    
    
    // SUNIONSTORE method.
    
    public function sunionstore($destination /* keys */ )
    {
        // Flatten the arguments.
        
        $this->validate_key($destination);
        $args = func_get_args(); array_shift($args);
        $keys = $this->array_flatten($args);
        
        // Expect: status.
        
        $this->write('SUNIONSTORE ' . $destination . ' ' . implode(' ', $keys));
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // SDIFF method.
    
    public function sdiff( /* keys */ )
    {
        // Flatten the arguments.
        
        $args = func_get_args();
        $keys = $this->array_flatten($args);
        
        // Expect: multi-bulk.
        
        $this->write('SDIFF ' . implode(' ', $keys));
        return $this->decode($this->get_response());
    }
    
    
    // SDIFFSTORE method.
    
    public function sdiffstore($destination /* keys */ )
    {
        // Flatten the arguments.
        
        $this->validate_key($destination);
        $args = func_get_args(); array_shift($args);
        $keys = $this->array_flatten($args);
        
        // Expect: status.
        
        $this->write('SDIFFSTORE ' . $destination . ' ' . implode(' ', $keys));
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // SMEMBERS method.
    
    public function smembers($key)
    {
        // Expect: multi-bulk.
        
        $this->validate_key($key);
        $this->write('SMEMBERS ' . $key);
        return $this->decode($this->get_response());
    }
    
    
    // SRANDMEMBER method.
    
    public function srandmember($key)
    {
        // Expect: bulk.
        
        $this->validate_key($key);
        $this->write('SRANDMEMBER ' . $key);
        return $this->decode($this->get_response());
    }
    
    
    // ZADD method. (Redis 1.1+)
    
    public function zadd($key, $score, $member)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.1)
        {
            throw new Exception('MSET is only supported in Redis 1.1+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Expect: integer (cast to bool).
        
        $this->validate_key($key);
        $mbc = $this->multi_bulk_command(array('ZADD', $key, $score, $this->encode($member)));
        if (!$mbc) return false;
        return (bool)$this->get_response();
    }
    
    
    // ZREM method. (Redis 1.1+)
    
    public function zrem($key, $score, $member)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.1)
        {
            throw new Exception('MSET is only supported in Redis 1.1+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Expect: integer (cast to bool).
        
        $this->validate_key($key);
        $mbc = $this->multi_bulk_command(array('ZREM', $key, $score, $this->encode($member)));
        if (!$mbc) return false;
        return (bool)$this->get_response();
    }
    
    
    // ZINCRBY method. (Redis 1.1+)
    
    public function zincrby($key, $increment, $member)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.1)
        {
            throw new Exception('MSET is only supported in Redis 1.1+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Expect: integer.
        
        $this->validate_key($key);
        $mbc = $this->multi_bulk_command(array('ZINCRBY', $key, $increment, $this->encode($member)));
        if (!$mbc) return false;
        return $this->get_response();
    }
    
    
    // ZRANGE method. (Redis 1.1+)
    
    public function zrange($key, $start, $end, $with_scores = false, $reverse = false)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.1)
        {
            throw new Exception('MSET is only supported in Redis 1.1+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Write the command. (Expect: multi-bulk)
        
        $this->validate_key($key);
        $command = $reverse ? 'ZREVRANGE' : 'ZRANGE';
        $command = $with_scores ? array($command, $key, $start, $end, 'WITHSCORES'): array($command, $key, $start, $end);
        $mbc = $this->multi_bulk_command($command);
        if (!$mbc) return false;
        
        // Return with scores.
        
        if ($with_scores)
        {
            $raw = $this->get_response();
            $count = count($raw);
            $return = array();
            for ($i = 0; $i < $count; $i += 2)
            {
                $return[$raw[$i]] = $this->decode($return[$raw[$i + 1]]);
            }
            return $return;
        }
        
        // Return plain elements.
        
        else
        {
            return $this->decode($this->get_response());
        }
    }
    
    
    // ZREVRANGE method. (Redis 1.1+)
    
    public function zrevrange($key, $start, $end, $with_scores)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.1)
        {
            throw new Exception('MSET is only supported in Redis 1.1+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Call zrange() with $reverse = true.
        
        return $this->zrange($key, $start, $end, $with_scores, true);
    }
    
    
    // ZRANGEBYSCORE method. (Redis 1.1+)
    
    public function zrangebyscore($key, $min, $max, $offset = false, $count = false)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.1)
        {
            throw new Exception('MSET is only supported in Redis 1.1+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Write the command.
        
        $this->validate_key($key);
        if ((int)$offset && (int)$count)
        {
            $command = array('ZRANGEBYSCORE', $key, $min, $max, 'LIMIT', (int)$offset, (int)$count);
        }
        else
        {
            $command = array('ZRANGEBYSCORE', $key, $min, $max);
        }
        $mbc = $this->multi_bulk_command($command);
        if (!$mbc) return false;
        
        // Expect: multi-bulk.
        
        return $this->decode($this->get_response());
    }
    
    
    // ZREMRANGEBYSCORE method. (Redis 1.1+)
    
    public function zremrangebyscore($key, $min, $max)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.1)
        {
            throw new Exception('MSET is only supported in Redis 1.1+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Expect: integer.
        
        $this->validate_key($key);
        $mbc = $this->multi_bulk_command(array('ZREMRANGEBYSCORE', $key, $min, $max));
        if (!$mbc) return false;
        return $this->get_response();
    }
    
    
    // ZCARD method. (Redis 1.1+)
    
    public function zcard($key)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.1)
        {
            throw new Exception('MSET is only supported in Redis 1.1+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Expect: integer.
        
        $this->validate_key($key);
        $this->write('ZCARD ' . $key);
        return $this->get_response();
    }
    
    
    // ZSCORE method. (Redis 1.1+)
    
    public function zscore($key, $member)
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.1)
        {
            throw new Exception('MSET is only supported in Redis 1.1+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Expect: bulk.
        
        $this->validate_key($key);
        $mbc = $this->multi_bulk_command(array('ZSCORE', $key, $this->encode($member)));
        if (!$mbc) return false;
        return $this->get_response();
    }
    
    
    // SORT method.
    
    public function sort($key, $conditions = '')
    {
        // Validate the key and the conditions.
        
        $this->validate_key($key);
        if (preg_match('[^\x21-\xfe]', $conditions)) throw new Exception('Illegal character in conditions: ' . $conditions);
    
        // Expect: multi-bulk.
        
        $this->write('SORT ' . $key . ' ' . $conditions . "\r\n", false);
        return $this->get_response();
    }
    
    
    // INFO method.
    
    public function info()
    {
        // Expect: bulk.
        
        $this->write('INFO');
        $info = $this->get_response();
        
        // Parse into an associative array.
        
        $info = explode("\n", $info);
        $return = array();
        
        foreach ($info as $line)
        {
            if (!$line) continue;
            $line = explode(':', $line, 2);
            $return[$line[0]] = trim($line[1]);
        }
        
        return $return;
    }
    
    
    // SAVE method.
    
    public function save()
    {
        // Expect: status.
        
        $this->write('SAVE');
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // BGSAVE method.
    
    public function bgsave()
    {
        // Expect: status.
        
        $this->write('BGSAVE');
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // BGREWRITEAOF method. (Redis 1.1+)
    
    public function bgrewriteaof()
    {
        // Check Redis version.
        
        if ($this->get_redis_version() < 1.1)
        {
            throw new Exception('MSET is only supported in Redis 1.1+. You are using Redis ' . $this->get_redis_version());
        }
        
        // Expect: status.
        
        $this->write('BGREWRITEAOF');
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // LASTSAVE method.
    
    public function lastsave($human_readable = false)
    {
        // Expect: integer.
        
        $this->write('LASTSAVE');
        $timestamp = $this->get_response();
        return $human_readable ? date('Y-m-d H:i:s', $timestamp) : $timestamp;
    }
    
    
    // FLUSHDB method.
    
    public function flushdb()
    {
        // Expect: status.
        
        $this->write('FLUSHDB');
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // FLUSHALL method.
    
    public function flushall()
    {
        // Expect: status.
        
        $this->write('FLUSHALL');
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // SHUTDOWN method.
    
    public function shutdown()
    {
        // Expect: status.
        
        $this->write('SHUTDOWN');
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // SLAVEOF method.
    
    public function slaveof($host = false, $port = 6379)
    {
        // If the host is not given, we're a master.
        
        $target = ($host !== false) ? ($host . ' ' . $port) : 'NO ONE';
        
        // Expect: status.
        
        $this->write('SLAVEOF ' . $target);
        return ($this->get_response() === true) ? true : false;
    }
    
    
    // MONITOR method.
    
    public function monitor()
    {
        // Not implemented.
        
        return false;
    }
    
    
    // Catch-all method for unknown commands.
    
    public function __call($name, $arguments)
    {
        // Assume multi-bulk command.
        
        array_unshift($arguments, strtoupper($name));
        $this->multi_bulk_command($arguments);
        return $this->get_response();
    }
    
    
    // Get response method. This method can parse anything that Redis says.
    
    private function get_response()
    {
        // Get the first byte of the response.
        
        $response = $this->readline();
        $type = $response[0];
        $message = substr($response, 1);
        
        // Switch by response type.
        
        switch ($type)
        {
            // Error : return the error message.
            
            case '-':
                
                return (string)$message;
            
            // Status : 'OK' is translated to true.
            
            case '+':
                
                if ($message === 'OK') return true;
                return $message;
            
            // Integer : return the number.
            
            case ':':
                
                return (int)$message;
            
            // Bulk : return the string.
            
            case '$':
                
                if ($message == -1) return false;
                return $this->read($message);
            
            // Multi-bulk : empty results are filled with false.
            
            case '*':
                
                $return = array();
                for ($i = 0; $i < $message; $i++)
                {
                    $header = $this->readline();
                    if ($header[0] !== '$')
                    {
                        throw new Exception('Unknown response received: ' . $header);
                    }
                    elseif ($header === '$-1')
                    {
                        $return[] = false;
                    }
                    else
                    {
                        $return[] = $this->read(substr($header, 1));
                    }
                }
                return $return;
                
            // Unknown response type.
            
            default:
                
                $this->disconnect();
                throw new Exception('Unknown response received: ' . $response);
        }
    }
    
    
    // Multi-bulk command sending method.  $force : false (no fallback), true (fallback), integer (fallback with a bulk portion).
    
    private function multi_bulk_command($elements, $force = 0)
    {
        // If multi-bulk commands are not available. (Redis 1.1+)
        
        if ($this->get_redis_version() < 1.1)
        {
            // Fall back to an old-style command.
            
            if ($force === true)
            {
                $command = implode(' ', $elements);
                return $this->write($command . "\r\n", false);
            }
            
            // Fall back to an old-style command, with a bulk portion.
            
            elseif ($force > 0)
            {
                $command = implode(' ', array_slice($elements, 0, $force));
                $command .= ' ' . strlen($elements[$force]) . "\r\n" . $elements[$force];
                return $this->write($command . "\r\n", false);                
            }
            
            // If $force is false, just return false.
            
            else
            {
                return false;
            }
        }
        
        // Otherwise, start writing a multi-bulk command.
        
        $command = '*' . count($elements) . "\r\n";
        
        // Add all the elements.
        
        foreach ($elements as $e)
        {
            $command .= '$' . strlen($e) . "\r\n" . $e . "\r\n";
        }
        
        // Write the command to the socket.
        
        return $this->write($command, false);
    }
    
    
    // Array flattening method, used by sinter() and family.
    
    private function array_flatten($array)
    {
        // Initialize the return value.
        
        $return = array();
        
        // Loop over the elements.
        
        foreach ($array as $a)
        {
            if (is_array($a))
            {
                foreach ($a as $b)
                {
                    $this->validate_key($b);
                    $return[] = $b;
                }
            }
            else
            {
                $this->validate_key($a);
                $return[] = $a;
            }
        }
        
        // Return.
        
        return $return;
    }
    
    
    // Serialization and compression subroutine.
    
    private function encode($data)
    {
        // If the data is not scalar, serialize it.
        
        if (!is_scalar($data))
        {
            $data = '#SERiALiZeD:' . serialize($data);
        }
        
        // If the data is bigger than the threshold, compress it.
        
        if ($this->compression && strlen($data) >= $this->compression)
        {
            $data = '&GziPPed:' . gzcompress($data);
        }
        
        // Return.
        
        return $data;
    }
    
    
    // Unserialization and decompression subroutine.
    
    private function decode($data)
    {
        // If the data is an array, decode recursively.
        
        if (is_array($data))
        {
            $count = count($data);
            for ($i = 0; $i < $count; $i++)
            {
                $data[$i] = $this->decode($data[$i]);
            }
            return $data;
        }
        
        // If the data seems compressed, decompress it.
        
        if (!strncmp($data, '&GziPPed:', 9))
        {
            $data = gzuncompress(substr($data, 9));
        }
        
        // If the data seems serialized, unserialize it.
        
        if (!strncmp($data, '#SERiALiZeD:', 12))
        {   
            $data = unserialize(substr($data, 12));
        }
        
        // Return the data.
        
        return $data;
    }
}
