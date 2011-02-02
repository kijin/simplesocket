<?php

/**
 * Memcached Client Library for PHP 5.2+
 * 
 * Based on Simple Socket Client, this library implements the text protocol
 * used by the Memcached caching server as well as by several other databases.
 * 
 * This library does not support multiple servers, because Simple Socket Client
 * doesn't. If you want to distribute keys across several Memcached instances,
 * use either of the two extensions mentioned above (they're faster anyway),
 * or use this library in combination with your own key distribution algorithm.
 * May the author suggest Distrib (http://github.com/kijin/distrib).
 * 
 * URL: http://github.com/kijin/simplesocket
 * Version: 0.1.7
 */

require_once(dirname(__FILE__) . '/../simplesocketclient.php');

/**
 * Copyright (c) 2010-2011, Kijin Sung <kijin.sung@gmail.com>
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

class MemcachedClient extends SimpleSocketClient
{
    /**
     * Configuration.
     */
    
    protected $compression = 256;
    protected $_default_host = '127.0.0.1';
    protected $_default_port = 11211;
    
    
    /**
     * Set compression threshold.
     */
    
    public function setCompressionThreshold($threshold = 256)
    {
        $this->compression = (int)$threshold ? (int)$threshold : false;
    }
    
    public function set_compression($threshold = 256) // Alias.
    {
        $this->compression = (int)$threshold ? (int)$threshold : false;
    }
    
    
    /**
     * GET : Retrieve an item from the server.
     * 
     * @param   string  The key.
     * @param   float   The variable to store the CAS token. [optional]
     * @return  mixed   The value, or false if the key does not exist.
     */
    
    public function get($key, &$cas_token = false)
    {
        // If the key is an array, pass on to getmulti().
        
        if (is_array($key)) return $this->getmulti($key);

        // Validate the key.
        
        $this->validate_key($key);
        
        // Try block.
        
        try
        {
            // Write the command.
            
            $command = ($cas_token !== false) ? 'gets ' : 'get ';
            $this->write($command . $key);
            
            // Read the response.
            
            $response = $this->readline();
            if ($response == 'END') return false;
            
            // Parse the response.
            
            $response = explode(' ', $response);
            if ($response[0] != 'VALUE') return false;
            $flag = $response[2];
            $length = $response[3];
            $cas_token = isset($response[4]) ? $response[4] : null;
            
            // Read the data.
            
            $data = $this->read($length);
            $data = $this->decode($data, $flag);
            
            // Read 'END'.
            
            $end = $this->readline();
            
            // Return the data.
            
            return $data;
        }
        
        // Return false on error.
        
        catch (Exception $e)
        {
            trigger_error('Memcached: ' . $e->getMessage(), E_USER_NOTICE);
            return false;
        }
    }
    
    
    /**
     * GETMULTI : Retrieve multiple items from the server.
     * 
     * @param   array  An array of keys, e.g. array('key1', 'key2');
     * @return  array  An array of keys and their corresponding values.
     */
    
    public function getmulti($keys)
    {
        // Validate the keys.
        
        foreach ($keys as $key) $this->validate_key($key);
        
        // Initialize the return array.
        
        $return = array();
        
        // Try block.
        
        try
        {
            // Write the command.
            
            $this->write('get ' . implode(' ', $keys));
            
            // Read multiple responses until 'END'.
            
            while (true)
            {
                // Read the response.
                
                $response = $this->readline();
                if ($response == 'END') break;
                
                // Parse the response.
                
                $response = explode(' ', $response);
                $key = $response[1];
                $flag = $response[2];
                $length = $response[3];
                
                // Read the data.
                
                $data = $this->read($length);
                $data = $this->decode($data, $flag);
                
                // Add to the return array.
                
                $return[$key] = $data;
            }
            
            // Return.
            
            return $return;
        }
        
        // Return false on error.
        
        catch (Exception $e)
        {
            trigger_error('Memcached: ' . $e->getMessage(), E_USER_NOTICE);
            return false;
        }
    }
    
    
    /**
     * SET : Store an item in the server.
     * 
     * @param   string  The key.
     * @param   mixed   The value.
     * @param   int     Expiry, in seconds.
     * @return  bool    True on success, false on failure.
     */
    
    public function set($key, $value, $expiry = 0)
    {
        // Call the common subroutine.
        
        return $this->store('set', $key, $value, $expiry);
    }
    
    
    /**
     * ADD : Store an item in the server, only if the key doesn't exist.
     * 
     * @param   string  The key.
     * @param   mixed   The value.
     * @param   int     Expiry, in seconds.
     * @return  bool    True on success, false on failure.
     */
    
    public function add($key, $value, $expiry = 0)
    {
        // Call the common subroutine.
        
        return $this->store('add', $key, $value, $expiry);
    }
    
    
    /**
     * REPLACE : Store an item in the server, only if the key already exists.
     * 
     * @param   string  The key.
     * @param   mixed   The value.
     * @param   int     Expiry, in seconds.
     * @return  bool    True on success, false on failure.
     */
    
    public function replace($key, $value, $expiry = 0)
    {
        // Call the common subroutine.
        
        return $this->store('replace', $key, $value, $expiry);
    }
    
    
    /**
     * APPEND : Append to existing value; don't use with compressed strings!
     * 
     * @param   string  The key.
     * @param   mixed   The value.
     * @param   int     Expiry, in seconds.
     * @return  bool    True on success, false on failure.
     */
    
    public function append($key, $value)
    {
        // Call the common subroutine.
        
        return $this->store('append', $key, $value, 0);
    }
    
    
    /**
     * PREPEND : Prepend to existing value; don't use with compressed strings!
     * 
     * @param   string  The key.
     * @param   mixed   The value.
     * @param   int     Expiry, in seconds.
     * @return  bool    True on success, false on failure.
     */
    
    public function prepend($key, $value)
    {
        // Call the common subroutine.
        
        return $this->store('prepend', $key, $value, 0);
    }
    
    
    /**
     * CAS : Check and Set an item in the server.
     * 
     * @param   float   The CAS token.
     * @param   string  The key.
     * @param   mixed   The value.
     * @param   int     Expiry, in seconds.
     * @return  bool    True on success, false on failure.
     */
    
    public function cas($cas_token, $key, $value, $expiry = 0)
    {
        // Call the common subroutine.
        
        return $this->store('cas', $key, $value, $expiry, $cas_token);
    }
    
    
    /**
     * Common subroutine for set(), add(), replace(), append(), prepend().
     * 
     * @param   string  The command.
     * @param   string  The key.
     * @param   mixed   The value.
     * @param   int     Expiry, in seconds.
     * @return  bool    True on success, false on failure.
     */
    
    protected function store($command, $key, $value, $expiry, $cas_token = null)
    {
        // Validate the key.
        
        $this->validate_key($key);
        
        // Serialize and/or compress the data.
        
        list($flag, $value) = $this->encode($value);
        
        // Try block.
        
        try
        {
            // Write the command and the value together.
            
            $command = $this->build_command($command, $key, $flag, $expiry, strlen($value));
            if ($cas_token !== null) $command .= ' ' . $cas_token;
            $this->write($command . "\r\n" . $value . "\r\n", false);
            
            // Read the response.
            
            $response = $this->readline();
            return ($response === 'STORED') ? true : false;        
        }
        
        // Return false on error.
        
        catch (Exception $e)
        {
            trigger_error('Memcached: ' . $e->getMessage(), E_USER_NOTICE);
            return false;
        }
    }
    
    
    /**
     * INCR : Increment an integer value.
     * 
     * If the key doesn't exist, zero (0) will be assumed.
     * 
     * @param   string  The key.
     * @param   int     The increment. [optional: default is 1]
     * @return  int     The new value.
     */
    
    public function incr($key, $diff = 1)
    {
        // Validate the key.
        
        $this->validate_key($key);
        
        // Try block.
        
        try
        {
            // Write the command and read the response.
            
            $this->write('incr ' . $key . ' ' . (int)$diff);
            $response = $this->readline();
            
            // If unsuccessful, force set to $diff.
            
            if (!ctype_digit($response))
            {
                $this->store('set', $key, $diff, 0);
                return $diff;
            }
            
            // Otherwise, return the new value.
            
            return $response;
        }
        
        // Return false on error.
        
        catch (Exception $e)
        {
            trigger_error('Memcached: ' . $e->getMessage(), E_USER_NOTICE);
            return false;
        }
    }
    
    
    /**
     * DECR : Decrement an integer value.
     * 
     * The new value will not go below zero (0).
     * 
     * @param   string  The key.
     * @param   int     The decrement. [optional: default is 1]
     * @return  int     The new value.
     */
    
    public function decr($key, $diff = 1)
    {
        // Validate the key.
        
        $this->validate_key($key);
        
        // Try block.
        
        try
        {
            // Write the command and read the response.
            
            $this->write('decr ' . $key . ' ' . (int)$diff);
            $response = $this->readline();
            
            // If unsuccessful, force set to 0.
            
            if (!ctype_digit($response))
            {
                $this->store('set', $key, 0, 0);
                return 0;
            }
            
            // Otherwise, return the new value.
            
            return $response;        
        }
        
        // Return false on error.
        
        catch (Exception $e)
        {
            trigger_error('Memcached: ' . $e->getMessage(), E_USER_NOTICE);
            return false;
        }
    }
    
    
    /**
     * DELETE : delete a key from the server.
     * 
     * @param   string  The key to delete.
     * @return  bool    True on success, false on failure.
     */
    
    public function delete($key)
    {
        // Validate the key.
        
        $this->validate_key($key);
        
        // Try block.
        
        try
        {
            // Write the command and read the response.
            
            $this->write('delete ' . $key);
            $response = $this->readline();
            return ($response === 'DELETED') ? true : false;
        }
        
        // Return false on error.
        
        catch (Exception $e)
        {
            trigger_error('Memcached: ' . $e->getMessage(), E_USER_NOTICE);
            return false;
        }
    }
    
    
    /**
     * FLUSH : delete all keys from the server.
     * 
     * @param   int   Delay, in seconds. [optional: default is 0]
     * @return  bool  True on success, false on failure.
     */
    
    public function flush($delay = 0)
    {
        // Validate the delay.
        
        $delay = (int)$delay;
        
        // Try block.
        
        try
        {
            // Write the command and read the response.
            
            $this->write('flush_all ' . (int)$delay);
            $response = $this->readline();
            return ($response === 'OK') ? true : false;
        }
        
        // Return false on error.
        
        catch (Exception $e)
        {
            trigger_error('Memcached: ' . $e->getMessage(), E_USER_NOTICE);
            return false;
        }
    }
    
    
    /**
     * STATS : display statistics about the server.
     * 
     * @return  array  An array containing server statistics.
     */
    
    public function stats()
    {
        // Initialize the return array.
        
        $return = array();
        
        // Try block.
        
        try
        {
            // Write the command.
            
            $this->write('stats');
            
            // Read multiple responses until 'END'.
            
            while (true)
            {
                // Read the response.
                
                $response = $this->readline();
                if ($response == 'END') break;
                $response = explode(' ', $response);
                
                // Add to the return array.
                
                $return[$response[1]] = $response[2];
            }
            
            // Return.
            
            return $return;
        }
        
        // Return false on error.
        
        catch (Exception $e)
        {
            trigger_error('Memcached: ' . $e->getMessage(), E_USER_NOTICE);
            return false;
        }
    }
    
    
    /**
     * Serialization and compression subroutine.
     * 
     * This method produces flags compatible with the Memcached extension.
     * 
     * @param   mixed  The data to serialize and/or compress.
     * @return  array  An array with 0 => flags, 1 => data.
     */
    
    protected function encode($data)
    {
        // Initialize the return array.
        
        $return = array(0, $data);
        
        // If the data is not scalar, serialize it.
        
        if (!is_scalar($return[1]))
        {
            $return[0] += 4;
            $return[1] = serialize($return[1]);
        }
        
        // If the data is bigger than the compression threshold, compress it.
        
        if ($this->compression > 0 && strlen($return[1]) >= $this->compression)
        {
            $return[0] += 16;
            $return[1] = gzcompress($return[1]);
        }
        
        // Return.
        
        return $return;
    }
    
    
    /**
     * Unserialization and decompression subroutine.
     * 
     * This method can parse flags produced by the default configurations of
     * either the Memcached extension or the Memcache extension, though
     * it *may* fail if an unconventional configuration was used.
     * 
     * @param   string  The data to unserialize and/or decompress.
     * @param   int     The flag returned from the server.
     * @return  string  The processed data.
     */
    
    protected function decode($data, $flag)
    {
        // Cast the flag to int.
        
        $flag = (int)$flag;
        
        // If the compression bit is set, decompress the data.
        
        if ($flag & 16 || $flag & 2)
        {
            $data = gzuncompress($data);
        }
        
        // If the serialization bit is set, unserialize the data.
        
        if ($flag & 4 || $flag & 1)
        {   
            if (!is_numeric($data)) $data = unserialize($data);
        }
        
        // Return the data.
        
        return $data;
    }
}
