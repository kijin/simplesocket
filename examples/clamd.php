<?php

/**
 * ClamAV-Daemon Client for PHP 5.2+
 * 
 * Based on Simple Socket Client, this library checks files for viruses by
 * connects to a clamav-daemon (clamd) instance.
 * 
 * If clamd is running on a UNIX socket (default: /var/run/clamav/clamd.ctl),
 * just instantiate the ClamdClient class without any arguments. Otherwise,
 * provide the hostname and the port (default: 3310) as arguments to the
 * class constructor.
 * 
 * To scan a file, call the scan() method with the filename as the argument.
 * This method will return 0 if the file is clean, 1 if the file is infected,
 * and 2 if an error occurs. The name of the virus, and the error description,
 * can be retrieved by calling last_virus() and last_error() respectively.
 * 
 * Example:
 * 
 * $upload_filename = $_FILES['upload']['tmp_name'];
 * if (!is_uploaded_file($upload_filename)) die('Hacking attempt!');
 * 
 * $clamd = new ClamdClient();
 * $upload_virus = $clamd->scan($upload_filename);
 * 
 * switch ($uploaded_virus)
 * {
 *     case 0: move_uploaded_file($upload_filename, $destination); break;
 *     case 1: die('Virus Found: ' . $clamd->last_virus());
 *     case 2: die('Error while scanning for viruses!');
 * }
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

class ClamdClient
{
    // Class constants.
    
    const OK = 0;
    const FOUND = 1;
    const ERROR = 2;
    
    
    // Host, port, and timeout.
    
    private $host = '';
    private $port = 0;
    private $timeout = 0;
    

    // Information about the last scan.
    
    private $last_virus = '';
    private $last_error = '';
    
    
    // Constructor override with the default host and port.
    
    public function __construct($host = false, $port = 3310, $timeout = 5)
    {
        // If no arguments given, assume UNIX socket.
        
        if ($host === false)
        {
            $host = '/var/run/clamav/clamd.ctl';
            $port = false;
        }
        
        // A quick check for IPv6 addresses (with colon).
        
        elseif (strpos($host, ':') !== false && strpos($host, '[') === false)
        {
            $host = '[' . $host . ']';
        }
        
        // Keep the connection info, but don't connect now.
        
        $this->host = $host;
        $this->port = $port;
        $this->timeout = $timeout;
    }
    
    
    // Scan method. Currently this only works for a single file.
    
    public function scan($filename)
    {
        // Check if the filename contains potentially problematic characters.
        
        if (preg_match('/[\\x00\\r\\n]/', $filename)) throw new Exception('Illegal filename: ' . $filename);
        
        // Check if the file exists and is readable.
        
        $filename = realpath($filename);
        if (!$filename || !file_exists($filename) || !is_file($filename)) throw new Exception($filename . ' does not exist');
        if (!is_readable($filename)) throw new Exception($filename . ' is not readable');
        
        // Connect to clamd, send the SCAN command, read the response, and disconnect.
        
        $socket = new SimpleSocketClient($this->host, $this->port, $this->timeout);
        $socket->write('SCAN ' . $filename);
        $response = $socket->readline();
        $response = trim($response);
        $socket->disconnect();
        
        // If the response starts with the filename, it's a valid response.
        
        if (!strncmp($response, $filename . ':', strlen($filename) + 1))
        {
            // Cut the filename from the response.
            
            $response = substr($response, strlen($filename) + 2);
            
            // OK
            
            if ($response === 'OK') return self::OK;
            
            // FOUND
            
            if (substr($response, strlen($response) - 5) === 'FOUND')
            {
                $this->last_virus = substr($response, 0, strlen($response) - 6);
                return self::FOUND;
            }
            
            // ERROR
            
            if (substr($response, strlen($response) - 5) === 'ERROR')
            {
                $this->last_error = substr($response, 0, strlen($response) - 6);
                return self::ERROR;
            }
        }
        
        // Otherwise, return error.
        
        $this->last_error = $response;
        return self::ERROR;
    }
    
    
    // Get the name of the last found virus.
    
    public function last_virus()
    {
        return $this->last_virus;
    }
    
    
    // Get details about the last error message.
    
    public function last_error()
    {
        return $this->last_error;
    }
}
