<?php

/**
 * Redis Client Library for PHP 5.2+
 * 
 * Based on Simple Socket Client, this library implements the text protocol
 * used by the Redis key-value store. It uses the multi-bulk command format,
 * which means you need Redis 1.2 or higher. Redis 2.0 is recommended.
 * 
 * Features:
 *   - Automatic serialization of non-scalar values.
 *   - Automatic compression of large values.
 *   - Incremental streaming of large result sets.
 *   - Pipelining of multiple commands.
 * 
 * IMPORTANT: Redis 1.0 is no longer supported.
 * 
 * If you attempt to use a command that is not supported by your version of
 * Redis, you will get a RedisException. The same will happen if the server
 * returns any other error, e.g. incorrect number of arguments.
 * 
 * If you attempt to store an array or object where only strings are allowed,
 * the array or object will be automatically serialized. When the value is
 * later fetched, it will be automatically unserialized. But this process adds
 * idiosyncratic headers to the value, which might not be compatible with other
 * client libraries. If interoperation is important for you, manually serialize
 * all your values using something like json_encode().
 * 
 * Compression can help you save RAM and disk space when storing large amounts
 * of text. But this also breaks compatibility with other client libraries, so
 * compression is disabled by deault. Call enableCompression() to enable it.
 * If called without an argument, a compression threshold of 1KB will apply.
 * 
 * Incremental streaming helps save RAM when working with large result sets.
 * It is disabled by default; to enable, call enableStreaming(). All methods
 * will behave in exactly the same way, except that multi-bulk responses will
 * be converted to a RedisStream object which implements the Iterator interface.
 * You can access the result set by calling fetch() on this object until it
 * returns false, or you can use a foreach() loop on it.
 * 
 * Pipelining allows you to send multiple commands to the server without having
 * to wait for the response. To open a pipeline, call openPipeline(). Subsequent
 * method calls will return the number of outstanding responses, instead of the
 * actual response. When you're ready to fetch responses, call fetchResponse()
 * as many times as there are outstanding responses. Then call closePipeline()
 * to return to the normal mode. If the pipeline is still clogged with unfetched
 * responses, you should call closePipeline(true) to flush the pipeline.
 * 
 * This library does not support multiple servers, nor any distribution method.
 * If you want to distribute keys across several Redis instances, use a more
 * fully featured client library (there are quite a few out there, you know);
 * or use this in combination with a thid-party key distribution library.
 * May the author suggests Distrib (http://github.com/kijin/distrib).
 * 
 * URL: http://github.com/kijin/simplesocket
 * Version: 0.2.4
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
    // Configuration and state data.
    
    protected $compression = false;
    protected $streaming = false;
    protected $pipeline_history = false;
    protected $multi_history = false;
    protected $last_status = false;
    
    
    // Enable compression and set the lower threshold. Default: 1KB.
    
    public function enableCompression($threshold = 1024)
    {
        $this->compression = (int)$threshold;
    }
    
    
    // Enable streaming.
    
    public function enableStreaming()
    {
        $this->streaming = true;
    }
    
    
    // Open the pipeline.
    
    public function openPipeline()
    {
        // Can't start two pipelines at the same time.
        
        if ($this->pipeline_history !== false) throw new RedisException('Pipeline mode already in effect.');

        // Enable pipeline mode, and initialize the history.
        
        $this->pipeline_history = array();
    }
    
    
    // Close the pipeline.
    
    public function closePipeline($force_flush = false)
    {
        // If the pipeline is still clogged, force flush or throw an exception.
        
        if (count($this->pipeline_history))
        {
            if ($force_flush)
            {
                while (count($this->pipeline_history)) $this->fetchResponse();
            }
            else
            {
                throw new RedisException('All responses must be read before the pipeline can be closed.');
            }
        }
        
        // Disable pipeline mode, and disable the history.
        
        $this->pipeline_history = false;
    }
    
    
    // Get the last status message.
    
    public function getLastStatus()
    {
        // This is usually one of 'OK', 'PONG', 'QUEUED'.
        
        return $this->last_status;
    }
    
    
    // All commands are caught by this magic method.
    
    public function __call($command, $args)
    {
        // Make the command uppercase.
        
        $command = strtoupper($command);
        
        // If the request needs pre-processing, do it here.
        
        switch ($command)
        {
            // MSET[NX]: flatten the dict into a list of alternating keys and values.
            
            case 'MSET':
            case 'MSETNX':
                $args = $this->preMSET($args[0]);
                break;
            
            // HMSET[NX]: keep the first arg, and flatten the rest.
            
            case 'HMSET':
            case 'HMSETNX':
                $args = $this->preMSET($args[1], $args[0]);
                break;
                
            // All other commands: flatten any arrays.
            
            default:
            switch (count($args))
            {
                case 0: break;
                
                case 1:
                    if (is_array($args[0])) $args = $args[0];
                    break;
                    
                default:
                    if (is_array($args[0]) || is_array($args[1])) $args = $this->flatten($args);
            }
        }
        
        // Start writing the multi-bulk request.
        
        $request = '*' . (1 + count($args)) . "\r\n";
        $request .= '$' . strlen($command) . "\r\n" . $command . "\r\n";
        
        // Append all the arguments, serializing/compressing them if necessary.
        
        foreach ($args as $argument)
        {
            $argument = $this->encode($argument);
            $request .= '$' . strlen($argument) . "\r\n" . $argument . "\r\n";
        }
        
        // Send the request to the server.
        
        $this->write($request, false);
        
        // Create a history entry. If getting multiple values, save the keys in the history, too.
        
        $history = array($command, false);
        if ($command === 'MGET') $history[1] = $args;
        if ($command === 'HMGET') $history[1] = array_slice($args, 1);
        
        // If a MULTI block is already open, add the current command to the MULTI history.
        
        if ($this->multi_history !== false) $this->multi_history[] = $history;
        
        // If this is a MULTI or DISCARD command, initialize or destroy the MULTI history.
        
        if ($command === 'MULTI') $this->multi_history = array();
        if ($command === 'DISCARD') $this->multi_history = false;
        
        // If a pipeline is open, add the current command to the pipeline history and return the pipeline counter.
        
        if ($this->pipeline_history !== false)
        {
            $this->pipeline_history[] = $history;
            return count($this->pipeline_history);
        }
        
        // Otherwise, just fetch and return the response.
        
        return $this->fetchResponse($history[0], $history[1]);
    }
    
    
    // Request pre-processing for MSET/HMSET.
    
    protected function preMSET($args, $key = false)
    {
        // Make sure we have an array of arguments.
        
        if (!is_array($args)) throw new RedisException('MSET/HMSET requires an array of arguments.');
        
        // Flatten the keys and values together.
        
        $return = ($key === false) ? array() : array($key);
        foreach ($args as $key => $value)
        {
            $return[] = $key;
            $return[] = $value;
            echo "$key $value \n";
        }
        return $return;
    }
    
    
    // Response post-processing for INFO.
    
    protected function postINFO($response)
    {
        // Construct an associative array.
        
        $response = explode("\n", $response);
        $return = array();
        foreach ($response as $line)
        {
            if (!$line) continue;
            $line = explode(':', $line, 2);
            $return[$line[0]] = trim($line[1]);
        }
        return $return;
    }
    
    
    // Response post-processing for MGET/HMGET.
    
    protected function postMGET($keys, $values)
    {
        // Construct an associative array.
        
        $return = array();
        $count = count($keys);
        for ($i = 0; $i < $count; $i++)
        {
            $return[$keys[$i]] = $values[$i];
        }
        return $return;
    }
    
    
    // Response post-processing for HGETALL.
    
    protected function postHGETALL($values)
    {
        // Construct an associative array.
        
        $return = array();
        $count = count($values);
        for ($i = 0; $i < $count; $i += 2)
        {
            $return[$values[$i]] = $values[$i + 1];
        }
        return $return;
    }
    
    
    // Fetch response method. This method can parse anything that Redis says.
    
    public function fetchResponse($command = false, $args = false)
    {
        // If command/args are not given, we're fetching from the pipeline.
        
        if ($command === false && $args === false && $this->pipeline_history !== false)
        {
            $history = array_shift($this->pipeline_history);
            if (!$history) throw new RedisException('No more responses in the pipeline.');
            $command = $history[0];
            $args = $history[1];
        }
        
        // Grab the first byte of the response to decide which type it is.
        
        $firstline = $this->readline();
        $type = $firstline[0];
        $message = substr($firstline, 1);
        $response = null;
        
        // Parse the body of the response.
        
        switch ($type)
        {
            // Error : throw an exception with the error message.
            
            case '-':
                
                throw new RedisException($message);
            
            // Status : return true for normal responses, except others which must be kept for post-processing.
            
            case '+':
                
                $this->last_status = $message;
                if (in_array($message, array('OK', 'PONG', 'QUEUED'))) return true;
                $response = false;
                break;
            
            // Integer : return the boolean equivalent, except where an integer is expected.
            
            case ':':
                
                if (in_array($command, array('TTL', 'INCR', 'INCRBY', 'DECR', 'DECRBY', 'APPEND', 'HLEN', 'LLEN', 'SCARD', 'ZCARD', 'LASTSAVE')))
                {
                    return (int)$message;
                }
                else
                {
                    return (bool)$message;
                }
            
            // Bulk : return the string, or null on failure.
            
            case '$':
                
                $response = ($message === '-1') ? null : $this->decode($this->read($message));
                break;
            
            // Multi-bulk : empty results are filled with nulls.
            
            case '*':
                
                // Count the number of bulk items.
                
                $count = (int)$message;
                
                // If this is an EXEC command, create and return an array of all responses.
                
                if ($command === 'EXEC')
                {
                    $return = array();
                    for ($i = 0; $i < $count; $i++)
                    {
                        $multi_history = array_shift($this->multi_history);
                        $multi_command = $multi_history[0];
                        $multi_args = $multi_history[1];
                        $return[] = $this->fetchResponse($multi_command, $multi_args);
                    }
                    $this->multi_history = false;
                    return $return;
                }
                
                // If streaming is enabled, create a stream object and pass it to post-processing.
                
                if ($this->streaming && $count)
                {
                    $response = new RedisStream($this, $count);
                }
                
                // Otherwise, read the whole response into an array and pass it to post-processing.
                
                else
                {
                    $response = array();
                    for ($i = 0; $i < $count; $i++)
                    {
                        $header = $this->readline();
                        if ($header[0] !== '$')
                        {
                            throw new RedisException('Unexpected response from Redis: ' . $header);
                        }
                        elseif ($header === '$-1')
                        {
                            $response[] = null;
                        }
                        else
                        {
                            $response[] = $this->decode($this->read(substr($header, 1)));
                        }
                    }
                }
                
                break;
                
            // Unknown response type.
            
            default:
                
                throw new RedisException('Unexpected response from Redis: ' . $response);
        }
        
        // If the response needs post-processing, do it here.
        
        switch ($command)
        {
            // TYPE: return the name of the type.
            
            case 'TYPE':
                return ($this->last_status === 'none') ? false : $this->last_status;
            
            // KEYS: if old-style response is received, convert to array.
            
            case 'KEYS':
                return (is_scalar($response)) ? explode(' ', $response) : $response;
            
            // INFO: convert to associative array.
            
            case 'INFO':
                return $this->postINFO($response);
            
            // MGET & HMGET: convert list to dict, using keys from the method call.
            
            case 'MGET':
            case 'HMGET':
                if ($this->streaming)
                {
                    $response->setKeys($args);
                    break;
                }
                else
                {
                    return $this->postMGET($args, $response);
                }
            
            // HGETALL: convert list of alternating keys and values into a dict.
            
            case 'HGETALL':
                if ($this->streaming)
                {
                    $response->returnPairs();
                    break;
                }
                else
                {
                    return $this->postHGETALL($response);
                }
        }
        
        return $response;
    }
    
    
    // Array flattening method.
    
    public function flatten($array)
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
                    $return[] = $b;
                }
            }
            else
            {
                $return[] = $a;
            }
        }
        
        // Return.
        
        return $return;
    }
    
    
    // Serialization and compression subroutine.
    
    public function encode($data)
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
    
    public function decode($data)
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


/**
 * Redis Stream Class.
 * 
 * An instance of this class is returned when a command such as MGET returns a
 * multi-bulk response, and if enableStreaming() had previously been called.
 * Note that you must fetch all values before sending another command through
 * the same socket, or else call close() to finish off the stream. Otherwise,
 * subsequent commands may exhibit unexpected behavior because unfetched data
 * would be clogging the pipe.
 */

class RedisStream implements Iterator
{
    // Protected properties.

    protected $caller = null;
    protected $count = 0;
    protected $current = 0;
    protected $keys = null;
    protected $key = 0;
    protected $pairs = false;
    protected $closed = false;
    
    
    // Constructor override.
    
    public function __construct($caller, $count)
    {
        $this->caller = $caller;
        $this->count = $count;
    }
    
    
    // Return pairs. Only used with HGETALL.
    
    public function returnPairs()
    {
        $this->pairs = true;
    }
    
    // Set keys. Only used with MGET and HMGET.
    
    public function setKeys($keys)
    {
        $this->keys = $keys;
    }
    
    
    // Count method.
    
    public function count()
    {
        // Return the count.
        
        return $this->pairs ? ($this->count / 2) : $this->count;
    }
    
    
    // Iterator: Rewind.
    
    public function rewind()
    {
        // Reset pointer to 0.
        
        $this->current = 0;
    }
    
    
    // Iterator: Valid.
    
    public function valid()
    {
        // Return false when the end of the stream is reached.
        
        return !$this->closed;
    }
    
    
    // Iterator: Current.
    
    public function current()
    {
        // Set the key.
        
        if ($this->keys !== null)
        {
            $this->key = $this->keys[$this->current];
        }
        elseif ($this->pairs)
        {
            $this->key = $this->fetch();
        }
        else
        {
            $this->key = $this->current;
        }
        
        // Fetch the next bulk item.
        
        return $this->fetch();
    }
    
    
    // Iterator: Key.
    
    public function key()
    {
        // Return the key for the previous item.
        
        return $this->key;
    }
    
    
    // Iterator: Next.
    
    public function next()
    {
        // Do nothing here. Pointer advancement is handled by fetch().
        
        return 0;
    }
    
    
    // Fetch method.
    
    public function fetch()
    {
        // If the pointer is already at the end, return false.
        
        if ($this->closed) return false;
        
        // Read the next bulk item from the pipe.
        
        $bulk_header = $this->caller->readline();
        $bulk_length = (int)substr($bulk_header, 1);
        $bulk_body = ($bulk_length < 0) ? null : $this->caller->decode($this->caller->read($bulk_length));
        
        // Increment the counter. If this is the last item, mark the stream as closed.
        
        $this->current++;
        if ($this->current >= $this->count) $this->closed = true;
        
        // Return the bulk body, or null if the bulk doesn't exist.
        
        return $bulk_body;
    }
    
    
    // Close method.
    
    public function close()
    {
        // Loop until the end of the stream.
        
        if (!$this->closed) while ($this->fetch() !== false) { }
    }
    
    
    // Destructor.
    
    public function __destruct()
    {
        // If the stream has not been closed, close now.
        
        if (!$this->closed) $this->close();
    }
}


/**
 * Redis Exception Class.
 */

class RedisException extends Exception { }
