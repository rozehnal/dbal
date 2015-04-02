<?php

namespace Doctrine\DBAL\Driver\SQLRelay;

/**
* A Doctrine DBAL driver for the Oracle SQL Relay
*
* @author Pavel Rozehnal <pavel.rozehnal@dixonsretail.com>
* @since 2.0
* @link http://doc.dvgu.ru/db/sql_relay/
*/
class Driver implements \Doctrine\DBAL\Driver
{
    /**
     * {@inheritdoc}
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = array())
    {
        if (isset($params['host']) && $params['host'] != '') {
            $server = $params['host'];
        }
        
        if (isset($params['port'])) {
            $port = $params['port'];
        } else {
            $port = 8125;
        }
        
        if (isset($params['socket'])) {
            $socket = $params['socket'];
        } else {
            $socket = null;
        }
        
        if (isset($params['retry_time'])) {
            $retry_time = $params['retry_time'];
        } else {
            $retry_time = 1;
        }
        
        if (isset($params['number_of_tries'])) {
            $number_of_tries = $params['number_of_tries'];
        } else {
            $number_of_tries = 1;
        }
        
        return new Connection($server, $port, $socket, $username, $password, $retry_time, $number_of_tries);
    }
    
    /**
     * {@inheritdoc}
     */
    public function getDatabasePlatform()
    {
        return new \Doctrine\DBAL\Platforms\OraclePlatform();
    }

    /**
     * {@inheritdoc}
     */
    public function getSchemaManager(\Doctrine\DBAL\Connection $conn)
    {
        return new \Doctrine\DBAL\Schema\OracleSchemaManager($conn);
    }

    /**
     * {@inheritdoc}
     */
    public function getName()
    {
        return 'sqlrelay';
    }

    /**
     * {@inheritdoc}
     */
    public function getDatabase(\Doctrine\DBAL\Connection $conn)
    {
        $params = $conn->getParams();

        return $params['user'];
    }
}