<?php

namespace Neo4jTransfer;


use Everyman\Neo4j\Client;

class Neo4jConnection
{
    /** @var string */
    protected $host;
    /** @var integer */
    protected $port;
    /** @var string */
    protected $username;
    /** @var string */
    protected $password;

    public function __construct($host=null, $port=null, $username=null, $password=null)
    {
        $this
            ->setHost($host)
            ->setPort($port)
            ->setUsername($username)
            ->setPassword($password)
        ;
    }

    /**
     * Make Neo4j Client Connection.
     * 
     * @return Client
     */
    public function makeClient()
    {
        $client = new Client($this->getHost(), $this->getPort());
        $client->getTransport()->setAuth($this->getUsername(), $this->getPassword());
        return $client;
    }

    /**
     * Check if the host was set.
     *
     * @return boolean
     */
    public function hasHost()
    {
        return isset($this->host);
    }

    /**
     * Get the host.
     *
     * @return string|null
     */
    public function getHost()
    {
        if (!$this->hasHost()) {
            return null;
        }
        return $this->host;
    }

    /**
     * Set the host.
     *
     * @param string $value The new value.
     * @return $this
     */
    public function setHost($value)
    {
        $this->host = $value;
        return $this;
    }

    /**
     * Check if the port was set.
     *
     * @return boolean
     */
    public function hasPort()
    {
        return isset($this->port);
    }

    /**
     * Get the port.
     *
     * @return integer|null
     */
    public function getPort()
    {
        if (!$this->hasPort()) {
            return null;
        }
        return $this->port;
    }

    /**
     * Set the port.
     *
     * @param string $value The new value.
     * @return $this
     */
    public function setPort($value)
    {
        $this->port = isset($value) ? intval($value) : null;
        return $this;
    }

    /**
     * Check if the user name was set.
     *
     * @return boolean
     */
    public function hasUsername()
    {
        return isset($this->username);
    }

    /**
     * Get the user name.
     *
     * @return string|null
     */
    public function getUsername()
    {
        if (!$this->hasUsername()) {
            return null;
        }
        return $this->username;
    }

    /**
     * Set the user name.
     *
     * @param string $value The new value.
     * @return $this
     */
    public function setUsername($value)
    {
        $this->username = $value;
        return $this;
    }

    /**
     * Check if the password was set.
     *
     * @return boolean
     */
    public function hasPassword()
    {
        return isset($this->password);
    }

    /**
     * Get the password.
     *
     * @return string|null
     */
    public function getPassword()
    {
        if (!$this->hasPassword()) {
            return null;
        }
        return $this->password;
    }

    /**
     * Set the password.
     *
     * @param string $value The new value.
     * @return $this
     */
    public function setPassword($value)
    {
        $this->password = $value;
        return $this;
    }

}