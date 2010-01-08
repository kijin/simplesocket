<?php

/**
 * HTTP Client for PHP 5.2+
 * 
 * Based on Simple Socket Client, this library implements a basic HTTP 1.1
 * client. It can be used to send HTTP requests and receive responses.
 * This library supports form submissions and file uploads, and saves memory
 * even when uploading large files. (The file is never loaded fully in memory.)
 * When reading responses, this library automatically decodes chunked and/or
 * gzipped data. The get() and post() methods return the response body;
 * headers and cookies can be accessed by calling other methods afterwards.
 * 
 * Example:
 * 
 * $client = new HttpClient($url);
 * $client->add_form('title', $title);
 * $client->add_form('content', $content);
 * $client->add_file('attachment', $filename);
 * $client->add_cookie('cookie_name', $cookie_value);
 * $client->set_referer($referer);
 * $client->post();
 * 
 * if ($client->get_response_code() == 200)
 * {
 *     $body = $client->get_response_body();
 *     $type = $client->get_response_content_type();
 *     $charset = $client->get_response_charset();
 *     $cookies = $client->get_response_cookies();
 * }
 * 
 * URL: http://github.com/kijin/simplesocket
 * Version: 0.1.8
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

class HttpClient extends SimpleSocketClient
{
    // Information about the client.
    
    private $url = false;
    private $user_agent = 'Mozilla/5.0 (compatible; HttpClient/0.1.4)';
    private $referer = '';
    private $proxy_host = '';
    private $proxy_port = 0;
    
    
    // Information about the request.
    
    private $request_url = '';
    private $request_scheme = '';
    private $request_host = '';
    private $request_port = '';
    private $request_path = '';
    private $request_headers = array();
    private $request_cookies = array();
    private $request_forms = array();
    private $request_files = array();
    
    
    // Information about the response.
    
    private $response_code = '';
    private $response_body = '';
    private $response_headers = array();
    private $response_cookies = array();
    private $response_content_type = '';
    private $response_charset = '';
    private $response_expires = '';
    private $response_lastmod = '';
    private $response_date = '';
    
    
    // Constructor.
    
    public function __construct($url = false)
    {
        // Save the URL.
        
        $this->url = $url;
    }
    
    
    // Set the request timeout.
    
    public function set_timeout($timeout)
    {
        // Sanitize the timeout, and save to instance.
        
        $this->timeout = (int)$timeout;
    }
    
    
    // Set the user agent.
    
    public function set_user_agent($user_agent)
    {
        // Sanitize the string, and save to instance.
        
        $this->user_agent = preg_replace('/\\s+/', ' ', $user_agent);
    }
    
    
    // Set the referer.
    
    public function set_referer($referer)
    {
        // Sanitize the string, and save to instance.
        
        $this->referer = preg_replace('/\\s+/', ' ', $referer);
    }
    
    
    // Set the proxy.
    
    public function set_proxy($host, $port)
    {
        // Sanitize the string, and save to instance.
        
        if (!preg_match('/^[0-9a-z.:_-]+$/i', $host)) throw new Exception('Invalid host: ' . $host);
        if (intval($port) != $port) throw new Exception('Invalid port: ' . $port);
        $this->proxy_host = $host;
        $this->proxy_port = $port;
    }
    
    
    // Add a cookie.
    
    public function add_cookie($key, $value)
    {
        // Sanitize the key, and save to instance.
        
        if (!preg_match('/^[0-9a-z_-]+$/i', $key)) throw new Exception('Invalid key: ' . $key);
        $this->request_cookies[$key] = $value;
    }
    
    
    // Add a header.
    
    public function add_header($key, $value)
    {
        // Sanitize the key, and save to instance.
        
        if (!preg_match('/^[0-9a-z_-]+$/i', $key)) throw new Exception('Invalid key: ' . $key);
        if (array_key_exists($key, $this->request_headers))
        {
            $this->request_headers[$key][] = $value;
        }
        else
        {
            $this->request_headers[$key] = array($value);
        }        
    }
    
    
    // Add a form element.
    
    public function add_form($key, $value)
    {
        // Sanitize the key, and save to instance.
        
        if (!preg_match('/^[0-9a-z_-]+$/i', $key)) throw new Exception('Invalid key: ' . $key);
        if (array_key_exists($key, $this->request_forms))
        {
            $this->request_forms[$key][] = $value;
        }
        else
        {
            $this->request_forms[$key] = array($value);
        }
    }
    
    
    // Add a file.
    
    public function add_file($key, $filename, $name = false)
    {
        // Sanitize the key.
        
        if (!preg_match('/^[0-9a-z_-]+$/i', $key)) throw new Exception('Invalid key: ' . $key);
        
        // If the name is not given, default to the base name of the file.
        
        if (!$name) $name = basename($filename);
        
        // Check if the file exists and is readable.
        
        $filename = realpath($filename);
        if (!$filename || !file_exists($filename) || !is_file($filename)) throw new Exception($filename . ' does not exist');
        if (!is_readable($filename)) throw new Exception($filename . ' is not readable');
        
        // Get the file size.
        
        $size = filesize($filename);
        
        // Save to instance.
        
        if (array_key_exists($key, $this->request_files))
        {
            $this->request_files[$key][] = array($filename, $name, $size);
        }
        else
        {
            $this->request_files[$key] = array(array($filename, $name, $size));
        }
    }
    
    
    // Get response code.
    
    public function get_response_code()
    {
        // Return.
        
        return $this->response_code;
    }
    
    
    // Get response body.
    
    public function get_response_body()
    {
        // Return.
        
        return $this->response_body;
    }
    
    
    // Get response headers.
    
    public function get_response_headers($key = false)
    {
        // If $key is false, return all.
        
        if ($key === false) return $this->response_headers;
        
        // Otherwise, search for matching headers.
        
        $return = array();
        foreach ($this->response_headers as $header)
        {
            if (!strncasecmp($header, $key . ':', strlen($key) + 1))
            {
                $return[] = trim(substr($header, strlen($key) + 1));
            }
        }
        return $return;
    }
    
    
    // Get response cookies.
    
    public function get_response_cookies($key = false)
    {
        // If $key is false, return all.
        
        if ($key === false) return $this->response_cookies;
        
        // Otherwise, return only the matching cookie.
        
        if (array_key_exists($key, $this->response_cookies)) return $this->response_cookies($key);
        
        // If not found, return false.
        
        return false;
    }
    
    
    // Get response content type.
    
    public function get_response_content_type()
    {
        // Return.
        
        return $this->response_content_type;
    }
    
    
    // Get response charset.
    
    public function get_response_charset()
    {
        // Return.
        
        return $this->response_charset;
    }
    
    
    // Get response date.
    
    public function get_response_date()
    {
        // Return.
        
        return $this->response_date;
    }
    
    
    // Get response expires.
    
    public function get_response_expires()
    {
        // Return.
        
        return $this->response_expires;
    }
    
    
    // Get response lastmod.
    
    public function get_response_lastmod()
    {
        // Return.
        
        return $this->response_lastmod;
    }
    
    
    // HEAD.
    
    public function head($url = false, $payload = false)
    {
        // Request.
        
        if ($url === false) $url = $this->url;
        return $this->request('HEAD', $url, $payload);
    }
    
    
    // GET.
    
    public function get($url = false, $payload = false)
    {
        // Request.
        
        if ($url === false) $url = $this->url;
        return $this->request('GET', $url, $payload);
    }
    
    
    // POST.
    
    public function post($url = false, $payload = false)
    {
        // Request.
        
        if ($url === false) $url = $this->url;
        return $this->request('POST', $url, $payload);
    }
    
    
    // REQUEST method, where all the action is.
    
    public function request($method, $url, $payload = false)
    {
        // Check and parse the URL.
        
        if (preg_match('/^(http|https):\\/\\/([^\\/]+)\\/([^\\s]*)$/i', $url, $matches))
        {
            $this->request_scheme = strtolower($matches[1]);
            $this->request_host = strtolower($matches[2]);
            if (strpos($this->request_host, ':') !== false)
            {
                $this->request_host = explode(':', $this->request_host, 2);
                $this->request_port = $this->request_host[1];
                $this->request_host = $this->request_host[0];
            }
            else
            {
                $this->request_port = ($this->request_scheme === 'http') ? 80 : 443;
            }
            $this->request_path = '/' . $matches[3];
            $this->request_url = $url;
        }
        else
        {
            throw new Exception('Invalid URL: ' . $url);
        }
        
        // Set some defaults.
        
        $boundary = '';
        $encoding = '';
        $length = 0;
        
        // Assemble the GET/HEAD querystring.
        
        if (($method === 'GET' || $method === 'HEAD') && count($this->request_forms))
        {
            // Add all form values.
            
            $tmp = array();
            foreach($this->request_forms as $key => $values)
            {
                foreach ($values as $value)
                {
                    $tmp[] = $key . '=' . urlencode($value);
                }
            }
            $tmp = implode('&', $tmp);
            
            // Add to the URL.
            
            $this->request_url .= '?' . $tmp;
            $this->request_path .= '?' . $tmp;
        }
        
        // Assemble the POST payload, except when it has been specified directly.
        
        elseif ($method === 'POST' && (count($this->request_forms) || count($this->request_files)) && $payload === false)
        {
            // If there are files, use multipart/form-data.
            
            if (count($this->request_files))
            {
                // Create a boundary marker, and set the encoding.
                
                $boundary = md5(mt_rand() . ':' . mt_rand());
                $encoding = 'multipart/form-data';
                $payload = array();
                
                // Add all form values.
                
                foreach($this->request_forms as $key => $values)
                {
                    foreach ($values as $value)
                    {
                        $payload[] = '--' . $boundary;
                        $payload[] = 'Content-Disposition: form-data; name="' . $key . '"';
                        $payload[] = '';
                        $payload[] = $value;
                    }
                }
                
                // Add all files.
                
                foreach($this->request_files as $key => $values)
                {
                    foreach ($values as $value)
                    {
                        $payload[] = '--' . $boundary;
                        $payload[] = 'Content-Disposition: attachment; name="' . $key . '"; filename="' . $value[1]. '"';
                        $payload[] = 'Content-Type: application/octet-stream';
                        $payload[] = 'Content-Transfer-Encoding: binary';
                        $payload[] = '';
                        $payload[] = chr(0) . $value[2] . ':' . $value[0];
                    }
                }
                
                // Add the ending boundary.
                
                $payload[] = '--' . $boundary . '--';
                
                // Calculate the length.
                
                $length = 0;
                foreach ($payload as $line)
                {
                    if (strlen($line) && $line[0] === chr(0))
                    {
                        $line = explode(':', substr($line, 1), 2);
                        $length += (int)$line[0] + 2;
                    }
                    else
                    {
                        $length += strlen($line) + 2;
                    }
                }
            }
            
            // Otherwise, use x-www-form-urlencoded.
            
            else
            {
                // Set the encoding.
                
                $encoding = 'application/x-www-form-urlencoded';
                $payload = array();
                
                // Add all form values.
                
                foreach($this->request_forms as $key => $values)
                {
                    foreach ($values as $value)
                    {
                        $payload[] = $key . '=' . urlencode($value);
                    }
                }
                $payload = implode('&', $payload);
                
                // Get the length.
                
                $length = strlen($payload);
            }
        }
        
        // If payload has been specified directly, just get its length.
        
        elseif ($payload !== false)
        {
            $length = strlen($payload);
        }
        
        // Otherwise, proceed without any payload.
        
        else
        {
            $length = 0;
        }
        
        // Begin writing the request header.
        
        $headers = array();
        $headers[] = strtoupper($method) . ' ' . (empty($this->proxy_host) ? $this->request_path : $this->request_url) . ' HTTP/1.1';
        $headers[] = 'Host: ' . $this->request_host;
        $headers[] = 'Connection: close';
        $headers[] = 'Accept-Encoding: gzip';
        $headers[] = 'User-Agent: ' . $this->user_agent;
        
        // If POST, set the content type.
        
        if ($method === 'POST')
        {
            if ($encoding === 'multipart/form-data')
            {
                $headers[] = 'Content-Type: multipart/form-data; boundary=' . $boundary;
            }
            elseif ($length)
            {
                $headers[] = 'Content-Type: application/x-www-form-urlencoded';
            }
        }
        
        // Set the content length.
        
        if ($length) $headers[] = 'Content-Length: ' . $length;
        
        // Add the referer.
        
        if (!empty($this->referer)) $headers[] = 'Referer: ' . $this->referer;
        
        // Add all custom headers.
        
        foreach ($this->request_headers as $key => $values)
        {
            foreach ($values as $value)
            {
                $headers[] = $key . ': ' . $value;
            }
        }
        
        // Add all cookies.
        
        $cookies = array();
        foreach ($this->request_cookies as $key => $value)
        {
            $cookies[] = $key . '=' . $value;
        }
        if (count($cookies))
        {
            $headers[] = 'Cookie: ' . implode('; ', $cookies);
        }
        
        // Open the socket.
        
        $this->host = ($this->request_scheme === 'https' ? 'ssl://' : '') . $this->request_host;
        $this->port = (int)$this->request_port;
        $this->timeout = ($this->timeout > 0) ? $this->timeout : 10;
        $this->connect();
        
        // Print the header.
        
        $this->write(implode("\r\n", $headers) . "\r\n\r\n", false);
        
        // If the payload is an array, convert to the multipart/form-data format.
        
        if (is_array($payload))
        {
            // Print each line.
            
            foreach ($payload as $line)
            {
                // If attached file, read from the file and write to the socket.
                
                if (strlen($line) && $line[0] === chr(0))
                {
                    $line = explode(':', substr($line, 1), 2);
                    $fp = fopen($line[1], 'r');
                    stream_copy_to_stream($fp, $this->con);
                    fclose($fp);
                    fwrite($this->con, "\r\n");
                }
                
                // Otherwise, just write to the socket.
                
                else
                {
                    $this->write($line . "\r\n", false);
                }
            }
            
            // Print the final CRLF.
            
            $this->write("\r\n", false);
        }
        
        // If the payload is a string, just print it.
        
        elseif ($length)
        {
            $this->write($payload . "\r\n", false);
        }
        
        // Clean up.
        
        $this->request_headers = array();
        $this->request_cookies = array();
        $this->request_forms = array();
        $this->request_files = array();
        $this->response_code = '';
        $this->response_body = '';
        $this->response_headers = array();
        $this->response_cookies = array();
        $this->response_content_type = '';
        $this->response_charset = '';
        $this->response_expires = '';
        $this->response_lastmod = '';
        $this->response_date = '';
        unset($headers);
        unset($payload);
        unset($boundary);
        unset($encoding);
        unset($length);
        
        // Some information needed to interpret the response.
        
        $chunked = false;
        $gzip = false;
        $length = false;
        
        // Start reading the headers.
        
        while (true)
        {
            // Read one line at a time.
            
            $line = $this->readline();
            
            // If it's a blank line, headers end here.
            
            if (!$line) break;
            
            // Add to the headers list.
            
            $this->response_headers[] = $line;
            
            // If it's the status line, grab it.
            
            if (substr($line, 0, 5) === 'HTTP/')
            {
                $this->response_code = (int)substr($line, 9, 3);
                continue;
            }
            
            // Start parsing the header.
            
            $header = explode(':', $line, 2);
            $key = trim($header[0]);
            $value = trim($header[1]);
            
            // Date, expires, last modified.
            
            if (strtolower($key) === 'date') $this->response_date = $value;
            if (strtolower($key) === 'expires') $this->response_expires = $value;
            if (strtolower($key) === 'last-modified') $this->response_lastmod = $value;
            
            // Chunked, gzip, content length.
            
            if (strtolower($key) === 'transfer-encoding' && strtolower($value) === 'chunked') $chunked = true;
            if (strtolower($key) === 'content-encoding' && strtolower($value) === 'gzip') $gzip = true;
            if (strtolower($key) === 'content-length' && ctype_digit($value)) $length = (int)$value;
            
            // Content type and charset.
            
            if (strtolower($key) === 'content-type')
            {
                $ctype = explode(';', $value);
                if (count($ctype))
                {
                    $this->response_content_type = trim($ctype[0]);
                    array_shift($ctype);
                    foreach ($ctype as $entry)
                    {
                        $entry = strtolower(trim($entry));
                        if (!strncmp($entry, 'charset=', 8)) $this->response_charset = substr($entry, 8);
                    }
                }
            }
            
            // Cookies.
            
            if (strtolower($key) === 'set-cookie')
            {
                $pos1 = strpos($value, '=');
                $pos2 = strpos($value, ';');
                if ($pos2 === false) $pos2 = strlen($value);
                if ($pos1 !== false)
                {
                    $cookie_name = substr($value, 0, $pos1);
                    $cookie_value = substr($value, $pos1 + 1, $pos2 - $pos1 - 1);
                    $this->response_cookies[$cookie_name] = $cookie_value;
                }
            }
        }
        
        // If HEAD, quit here.
        
        if ($method === 'HEAD')
        {
            $this->disconnect();
            return true;
        }
        
        // Read chunked content.
        
        if ($chunked)
        {
            // Initialize the response payload.
            
            $this->response_body = '';
            
            // Loop until all chunks have been read.
            
            while (true)
            {
                // Read a line.
                
                $size = $this->readline();
                
                // If it says zero (0), we're at the end of the response.
                
                if ($size === '0')
                {
                    // Read until there are no more lines.
                    
                    while (true)
                    {
                        try { $this->readline(); }
                        catch (Exception $e) { break 2; }
                    }
                }
                
                // Get the chunk size.
                
                $comment = strpos($size, ';');
                if ($comment !== false) $size = substr($size, 0, $comment);
                $size = hexdec(trim($size));
                
                // Read the chunk data.
                
                $data = $this->read($size);
                if ($gzip && substr($data, 0, 3) === "\x1f\x8b\x08") $data = gzinflate(substr($data, 10));
                
                // Add to the response payload.
                
                $this->response_body .= $data;
            }
        }
        
        // Read regular content.
        
        else
        {
            $this->response_body = $this->read($length);
            if ($gzip && substr($this->response_body, 0, 3) === "\x1f\x8b\x08") $this->response_body = gzinflate(substr($this->response_body, 10));
        }
        
        // Disconnect.
        
        $this->disconnect();
        
        // Return the response body.
        
        return $this->response_body;
    }
}
