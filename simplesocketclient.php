<?php

/**
 * A Simple Socket Client for PHP 5.2+
 * 
 * This class implements a generic socket client. It can be extended to
 * create client libraries working with text-based protocols.
 * In addition to socket creation, reading, and writing capabilities,
 * this class also supports lazy loading (not connect until actually needed),
 * as well as basic key validation and command building helper methods.
 * 
 * URL: http://github.com/kijin/simplesocket
 * Version: 0.1.2
 * 
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

class SimpleSocketClient
{
    /**
     * Protected properties.
     * 
     * Although these properties can be manipulated directly by children,
     * it is best to keep them in the hands of the parent class.
     */
    
    protected $con = null;
    protected $host = '';
    protected $port = 0;
    protected $timeout = 0;
    
    
    /**
     * Constructor.
     * 
     * Host and port must be supplied at the time of instantiation.
     * 
     * @param  string  The hostname or the IP address of the server.
     * @param  int     The port of the server.
     * @param  int     Connection timeout in seconds. [optional: default is 5]
     */
    
    public function __construct($host, $port, $timeout = 5)
    {
        // A quick check for IPv6 addresses (with colon).
        
        if (strpos($host, ':') !== false) $host = '[' . $host . ']';
        
        // Keep the connection info, but don't connect now.
        
        $this->host = $host;
        $this->port = $port;
        $this->timeout = $timeout;
    }
    
    
    /**
     * Connect to the server.
     * 
     * Normally, this method is useful only for debugging purposes, because
     * it will be called automatically the first time a read/write operation
     * is attempted.
     */
    
    public function connect()
    {
        // If already connected, return true.
        
        if ($this->con !== null && $this->con !== false) return true;
        
        // Attempt to connect.
        
        $this->con = @stream_socket_client($this->host . ':' . $this->port, $errno, $errstr, $this->timeout);
        
        // If there's an error, set $con to false, and throw an exception.
        
        if (!$this->con)
        {
            $this->con = false;
            throw new Exception('Cannot connect to ' . $this->host . ' port ' . $this->port . ': ' . $errstr . ' (code ' . $errno . ')');
        }
        
        // Return true to indicate success.
        
        return true;
    }
    
    
    /**
     * Disconnect from the server.
     * 
     * Normally, this method is useful only for debugging purposes, because
     * it will be automatically called in the event of an error (resulting in
     * reconnection the next time a read/write operation is attempted), as
     * well as at the end of the execution of the script.
     */
    
    public function disconnect()
    {
        // Close the socket.
        
        @fclose($this->con);
        $this->con = null;
        
        // Return true to indicate success.
        
        return true;
    }
    
    
    /**
     * Generic read method.
     * 
     * This method reads a specified number of bytes from the socket.
     * By default, it will also read a CRLF sequence (2 bytes) in addition to
     * the specified number of bytes, and remove that CRLF sequence once it
     * has been read. This is useful for most text-based protocols; however,
     * if you do not want such behavior, pass additional 'false' arguments.
     * 
     * @param   int        The number of bytes to read, or -1 to read until EOF.
     * @param   bool       Whether or not to read CRLF at the end, too. [optional: default is true]
     * @param   bool       Whether or not to strip CRLF from the end of the response. [optional: default is true]
     * @return  string     Data read from the socket.
     * @throws  Exception  If an error occurs while reading from the socket.
     */
    
    protected function read($bytes = -1, $autonewline = true, $trim = true)
    {
        // If not connected yet, connect now.
        
        if ($this->con === null) $this->connect();
        if ($this->con === false)
        {
            throw new Exception('Cannot connect to ' . $this->host . ' port ' . $this->port);
        }
        
        // If $autonewline is true, read 2 more bytes.
        
        if ($autonewline && $bytes !== -1) $bytes += 2;
        
        // Read from the socket.
        
        $data = @stream_get_contents($this->con, $bytes);
        
        // If the result is false, throw an exception.
        
        if ($data === false)
        {
            $this->disconnect();
            throw new Exception('Cannot read ' . $bytes . ' bytes from ' . $this->host . ' port ' . $this->port);
        }
        
        // Otherwise, trim and return the data.
        
        if ($trim && substr($data, strlen($data) - 2) === "\r\n") $data = substr($data, 0, strlen($data) - 2);
        return $data;
    }
    
    
    /**
     * Generic readline method.
     * 
     * This method reads one line from the socket, i.e. it reads until it hits
     * a CRLF sequence. By default, that CRLF sequence will be removed from the
     * return value. This is useful for most text-based protocols; however,
     * if you do not want such behavior, pass an additional 'false' argument.
     * 
     * @param   bool       Whether or not to strip CRLF from the end of the response. [optional: default is true]
     * @return  string     Data read from the socket.
     * @throws  Exception  If an error occurs while reading from the socket.
     */
        
    protected function readline($trim = true)
    {
        // If not connected yet, connect now.
        
        if ($this->con === null) $this->connect();
        if ($this->con === false)
        {
            throw new Exception('Cannot connect to ' . $this->host . ' port ' . $this->port);
        }
        
        // Read a line from the socket.
        
        $data = @fgets($this->con);
        
        // If the result is false, throw an exception.
        
        if ($data === false)
        {
            $this->disconnect();
            throw new Exception('Cannot read a line from ' . $this->host . ' port ' . $this->port);
        }
        
        // Otherwise, trim and return the data.
        
        if ($trim && substr($data, strlen($data) - 2) === "\r\n") $data = substr($data, 0, strlen($data) - 2);
        return $data;
    }
    
    
    /**
     * Generic write method.
     * 
     * This method writes a string to the socket. By default, this method will
     * write a CRLF sequence in addition to the given string. This is useful
     * for most text-based protocols; however, if you do not want such behavior,
     * make sure to pass an additional 'false' argument.
     * 
     * @param   string     The string to write to the socket.
     * @param   bool       Whether or write CRLF in addition to the given string. [optional: default is true]
     * @return  bool       True on success.
     * @throws  Exception  If an error occurs while reading from the socket.
     */
        
    protected function write($string, $autonewline = true)
    {
        // If not connected yet, connect now.
        
        if ($this->con === null) $this->connect();
        if ($this->con === false)
        {
            throw new Exception('Cannot connect to ' . $this->host . ' port ' . $this->port);
        }
        
        // If $autonewline is true, add CRLF to the content.
        
        if ($autonewline) $string .= "\r\n";
        
        // Write the whole string to the socket.
        
        while ($string !== '')
        {
            // Start writing.
            
            $written = @fwrite($this->con, $string);
            
            // If the result is false, throw an exception.
            
            if ($written === false)
            {
                $this->disconnect();
                throw new Exception('Cannot write to ' . $this->host . ' port ' . $this->port);
            }
            
            // If nothing was written, return true to indicate success.
            
            if ($written == 0) return true;
            
            // Prepare the string for the next write.
            
            $string = substr($string, $written);
        }
        
        // Return true to indicate success.
        
        return true;
    }
    
    
    /**
     * Generic key validation method.
     * 
     * This method will throw an exception if:
     *   - The key is empty.
     *   - The key is more than 250 bytes long.
     *   - The key contains characters outside of the ASCII printable range.
     * 
     * @param   string     The key to validate.
     * @return  bool       True of the key is valid.
     * @throws  Exception  If the key is invalid.
     */
    
    protected function validate_key($key)
    {
        // Empty?
        
        if ($key === '') throw new Exception('Key is empty');
        
        // Check the length.
        
        if (strlen($key) > 250) throw new Exception('Key is too long: ' . $key);
        
        // Check for illegal characters.
        
        if (preg_match('/[^\\x21-\\x7e]/', $key)) throw new Exception('Illegal character in key: ' . $key);
        
        // Return true to indicate pass.
        
        return true;
    }
    
    
    /**
     * Generic command building method.
     * 
     * This method will accept one or more string arguments, and return them
     * all concatenated with one space between each. If this is convenient
     * for you, help yourself.
     * 
     * @param   string  As many arguments as you wish.
     * @return  string  The concatenated string.
     */
    
    protected function build_command( /* arguments */ )
    {
        // Fetch all the arguments.
        
        $args = func_get_args();
        
        // Glue and return.
        
        return implode(' ' , $args);
    }
    
    
    /**
     * Destructor.
     * 
     * Although not really necessary, the destructor will attempts to
     * disconnect in case something weird happens.
     */
    
    public function __destruct()
    {
        // Disconnect.
        
        @fclose($this->con);
    }
}
