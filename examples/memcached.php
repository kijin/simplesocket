<?php

// Load the simple socket client library.

require_once(dirname(__FILE__) . '/../simplesocketclient.php');

// Memcached client.

class MemcachedClient extends SimpleSocketClient
{
    // GET method.
    // Takes a key and returns the value as a string, or false on failure.
    
    public function get($key)
    {
        // If the key is an array, pass on to getmulti().
        
        if (is_array($key)) return $this->getmulti($key);

        // Validate the key.
        
        $this->validate_key($key);
        
        // Write the command.
        
        $this->write('get ' . $key);
        
        // Read the response.
        
        $response = $this->readline();
        $response = explode(' ', $response);
        $flag = $response[2];
        $length = $response[3];
        
        // Read the data.
        
        $data = $this->read($length);
        if ($flag == 2) $data = unserialize($data);
        
        // Read 'END'.
        
        $end = $this->readline();
        
        // Return the data.
        
        return $data;
    }
    
    // GET MULTI method.
    // Takes an array of keys, and returns an array of keys => values, or false on failure.
    
    public function getmulti($keys)
    {
        // Validate the keys.
        
        foreach ($keys as $key) $this->validate_key($key);
        
        // Initialize the return array.
        
        $return = array();
        
        // Write the command.
        
        $this->write('get ' . implode(' ', $keys));
        
        // Read multiple responses until 'END'.
        
        while (true)
        {
            // Read the response.
            
            $response = $this->readline();
            if ($response == 'END') break;
            $response = explode(' ', $response);
            $key = $response[1];
            $flag = $response[2];
            $length = $response[3];
            
            // Read the data.
            
            $data = $this->read($length);
            if ($flag) $data = unserialize($data);
            
            // Add to the return array.
            
            $return[$key] = $data;
        }
        
        // Return.
        
        return $return;
    }
    
    // SET method.
    // Returns true on success, false on failure.
    
    public function set($key, $value, $expiry = 0)
    {
        // Call the common subroutine.
        
        return $this->store('set', $key, $value, $expiry);
    }
    
    // ADD method.
    // Returns true on success, false on failure.
    
    public function add($key, $value, $expiry = 0)
    {
        // Call the common subroutine.
        
        return $this->store('add', $key, $value, $expiry);
    }
    
    // REPLACE method.
    // Returns true on success, false on failure.
    
    public function replace($key, $value, $expiry = 0)
    {
        // Call the common subroutine.
        
        return $this->store('replace', $key, $value, $expiry);
    }
    
    // APPEND method.
    // Returns true on success, false on failure.
    
    public function append($key, $value)
    {
        // Call the common subroutine.
        
        return $this->store('append', $key, $value, 0);
    }
    
    // PREPEND method.
    // Returns true on success, false on failure.
    
    public function prepend($key, $value)
    {
        // Call the common subroutine.
        
        return $this->store('prepend', $key, $value, 0);
    }
    
    // Common subroutine for all storage commands.
    // Returns true on success, false on failure.
    
    private function store($command, $key, $value, $expiry)
    {
        // Validate the key.
        
        $this->validate_key($key);
        
        // If the value is not scalar, serialize and set a flag.
        
        if (!is_scalar($value))
        {
            $value = serialize($value);
            $flag = 2;
        }
        else
        {
            $flag = 1;
        }
        
        // Write the command and the value together.
        
        $command = $this->build_command($command, $key, $flag, $expiry, strlen($value));
        $this->write($command . "\r\n" . $value . "\r\n", false);
        
        // Read the response.
        
        $response = $this->readline();
        return ($response === 'STORED') ? true : false;        
    }
    
    // INCR method.
    // Returns the new value, or false on failure.
    
    public function incr($key, $diff = 1)
    {
        // Validate the key.
        
        $this->validate_key($key);
        
        // Write the command and read the response.
        
        $this->write('incr ' . $key . ' ' . (int)$diff);
        $response = $this->readline();
        
        // If unsuccessful, force set to $diff.
        
        if (!ctype_digit($response))
        {
            $this->store('set', $key, $diff, 0);
            return $this->get($key);
        }
        
        // Otherwise, return the new value.
        
        return $response;
    }
    
    // DECR method.
    // Returns the new value, or false on failure.
    
    public function decr($key, $diff = 1)
    {
        // Validate the key.
        
        $this->validate_key($key);
        
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
    
    // DELETE method.
    // Returns true on success, false on failure.
    
    public function delete($key)
    {
        // Validate the key.
        
        $this->validate_key($key);
        
        // Write the command and read the response.
        
        $this->write('delete ' . $key);
        $response = $this->readline();
        return ($response === 'DELETED') ? true : false;
    }
    
    // FLUSH method.
    // Returns true on success, false on failure.
    
    public function flush($delay = 0)
    {
        // Validate the delay.
        
        $delay = (int)$delay;
        
        // Write the command and read the response.
        
        $this->write('flush_all ' . (int)$delay);
        $response = $this->readline();
        return ($response === 'OK') ? true : false;
    }
    
    // STATS method.
    // Returns an array containing statistics data.
    
    public function stats()
    {
        // Initialize the return array.
        
        $return = array();
        
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
}
