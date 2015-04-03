<?php

namespace Doctrine\DBAL\Driver\SQLRelay;

use Doctrine\DBAL\Platforms\OraclePlatform;

/**
 * Oracle SQL Relay implementation of the Connection interface.
 *
 * @author Pavel Rozehnal <pavel.rozehnal@dixonsretail.com>
 * @since 2.0
 */
class Connection implements \Doctrine\DBAL\Driver\Connection
{
    /**
     * @var resource
     */
    protected $dbh;

    /**
     * @param $server
     * @param $port
     * @param $socket
     * @param $user
     * @param $password
     * @param $retryTime
     * @param $numberOfTries
     * @param $engine
     * @throws Exception
     */
    public function __construct($server, $port, $socket, $user, $password, $retryTime, $numberOfTries)
    {
        $this->dbh = sqlrcon_alloc($server, $port, $socket, $user, $password, $retryTime, $numberOfTries);
        sqlrcon_autoCommitOff($this->dbh);
        //sqlrcon_autoCommitOn
        
        if ( !$this->dbh) {
            throw Exception::fromErrorInfo('SQL Relay conn error');
        }
    }
    
    public function __destruct() {
        if ( $this->dbh ) {
            sqlrcon_endSession($this->dbh);
            sqlrcon_free($this->dbh);
        }
    }
    

    /**
     * {@inheritdoc}
     */
    public function prepare($prepareString)
    {
        return new Statement($this->dbh, $prepareString, $this);
    }

    /**
     * {@inheritdoc}
     */
    public function query()
    {
        $args = func_get_args();
        $sql = $args[0];
        //$fetchMode = $args[1];
        $stmt = $this->prepare($sql);
        $stmt->execute();

        return $stmt;
    }

    /**
     * {@inheritdoc}
     */
    public function quote($value, $type=\PDO::PARAM_STR)
    {
        if (is_int($value) || is_float($value)) {
            return $value;
        }
        $value = str_replace("'", "''", $value);

        return "'" . addcslashes($value, "\000\n\r\\\032") . "'";
    }

    /**
     * {@inheritdoc}
     */
    public function exec($statement)
    {
        $stmt = $this->prepare($statement);
        $stmt->execute();

        return $stmt->rowCount();
    }

    /**
     * {@inheritdoc}
     */
    public function lastInsertId($name = null)
    {
        if ($name === null) {
            return false;
        }

        OraclePlatform::assertValidIdentifier($name);

        $sql    = 'SELECT ' . $name . '.CURRVAL FROM DUAL';
        $stmt   = $this->query($sql);
        $result = $stmt->fetch(\PDO::FETCH_ASSOC);

        if ($result === false || !isset($result['CURRVAL'])) {
            throw new Exception("lastInsertId failed: Query was executed but no result was returned.");
        }

        return (int) $result['CURRVAL'];
    }

    /**
     * {@inheritdoc}
     */
    public function beginTransaction()
    {
        //sqlrcon_begin($this->dbh);
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function commit()
    {
        sqlrcon_commit($this->dbh);
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function rollBack()
    {
        sqlrcon_rollback($this->dbh);
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function errorCode()
    {
        return -1;
    }

    /**
     * {@inheritdoc}
     */
    public function errorInfo()
    {
        return -1;
    }
}