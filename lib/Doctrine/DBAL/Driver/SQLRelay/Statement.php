<?php

namespace Doctrine\DBAL\Driver\SQLRelay;


use PDO;

/**
 * The Oracle SQL Relay implementation of the Statement interface.
 *
 * @since 2.0
 * @author Pavel Rozehnal <pavel.rozehnal@dixonsretail.com>
 */
class Statement implements \IteratorAggregate, \Doctrine\DBAL\Driver\Statement
{
    
    const BIND_OUTPUT_TYPE_CURSOR = 0,
        BIND_OUTPUT_TYPE_DOUBLE = 1,
        BIND_OUTPUT_TYPE_INTEGER = 2,
        BIND_OUTPUT_TYPE_STRING = 3,
        BIND_OUTPUT_TYPE_BLOB = 4,
        BIND_OUTPUT_TYPE_CLOB = 5;
    
    /**
     * @var resource
     */
    protected $_dbh;

    /**
     * @var resource
     */
    protected $_sth;

    /**
     * @var \Doctrine\DBAL\Driver\OCI8\OCI8Connection
     */
    protected $_conn;
    
    /**
     * @var int sqlrcurref
     */
    protected $_cur;

    /**
     * @var string
     */
    protected static $_PARAM = ':param';

    /**
     * @var array
     */
    protected static $fetchModeMap = array(
        PDO::FETCH_BOTH => OCI_BOTH,
        PDO::FETCH_ASSOC => OCI_ASSOC,
        PDO::FETCH_NUM => OCI_NUM,
        PDO::FETCH_COLUMN => OCI_NUM,
    );

    /**
     * @var integer
     */
    protected $_defaultFetchMode = PDO::FETCH_BOTH;

    /**
     * @var array
     */
    protected $_paramMap = array();
    
    /**
     * @var integer
     */
    protected $_currentRow = 0;
    
    /**
     *
     * @var array
     */
    private $_outputBindVars = array();
    

    /**
     * Creates a new OCI8Statement that uses the given connection handle and SQL statement.
     *
     * @param resource                                  $dbh       The connection handle.
     * @param string                                    $statement The SQL statement.
     * @param \Doctrine\DBAL\Driver\OCI8\OCI8Connection $conn
     */
    public function __construct($dbh, $statement, Connection $conn, $cur = null)
    {
        
        if ($cur === null) {
            list($statement, $paramMap) = self::convertPositionalToNamedPlaceholders($statement);
            $this->_cur=sqlrcur_alloc($dbh);
            sqlrcur_prepareQuery($this->_cur, $statement);
            $this->_paramMap = $paramMap;
        }else{
            $this->_cur=$cur;
        }
        
        $this->_dbh = $dbh;
        $this->_conn = $conn;
    }

    /**
     * Converts positional (?) into named placeholders (:param<num>).
     *
     * Oracle does not support positional parameters, hence this method converts all
     * positional parameters into artificially named parameters. Note that this conversion
     * is not perfect. All question marks (?) in the original statement are treated as
     * placeholders and converted to a named parameter.
     *
     * The algorithm uses a state machine with two possible states: InLiteral and NotInLiteral.
     * Question marks inside literal strings are therefore handled correctly by this method.
     * This comes at a cost, the whole sql statement has to be looped over.
     *
     * @todo extract into utility class in Doctrine\DBAL\Util namespace
     * @todo review and test for lost spaces. we experienced missing spaces with oci8 in some sql statements.
     *
     * @param string $statement The SQL statement to convert.
     *
     * @return string
     */
    static public function convertPositionalToNamedPlaceholders($statement)
    {
        $count = 1;
        $inLiteral = false; // a valid query never starts with quotes
        $stmtLen = strlen($statement);
        $paramMap = array();
        for ($i = 0; $i < $stmtLen; $i++) {
            if ($statement[$i] == '?' && !$inLiteral) {
                // real positional parameter detected
                $paramMap[$count] = ":param$count";
                $len = strlen($paramMap[$count]);
                $statement = substr_replace($statement, ":param$count", $i, 1);
                $i += $len-1; // jump ahead
                $stmtLen = strlen($statement); // adjust statement length
                ++$count;
            } else if ($statement[$i] == "'" || $statement[$i] == '"') {
                $inLiteral = ! $inLiteral; // switch state!
            }
        }

        return array($statement, $paramMap);
    }

    /**
     * {@inheritdoc}
     */
    public function bindValue($param, $value, $type = null)
    {
        return $this->bindParam($param, $value, $type, null);
        //sqlrcur_defineOutputBind($this->_cur, $param, 20);
    }

    /**
     * {@inheritdoc}
     */
    public function bindParam($column, &$variable = null, $type = null, $length = null)
    {
        $column = isset($this->_paramMap[$column]) ? $this->_paramMap[$column] : $column;
        switch ($type) {
            case self::BIND_OUTPUT_TYPE_CURSOR:
                $this->_outputBindVars[$column] = array('type'=>$type, 'var'=> &$variable);
                return sqlrcur_defineOutputBindCursor($this->_cur, $column);
            case self::BIND_OUTPUT_TYPE_DOUBLE:
                $this->_outputBindVars[$column] = array('type'=>$type, 'var'=> &$variable);
                return sqlrcur_defineOutputBindDouble($this->_cur, $column);
            case self::BIND_OUTPUT_TYPE_INTEGER:
                $this->_outputBindVars[$column] = array('type'=>$type, 'var'=> &$variable);
                return sqlrcur_defineOutputBindInteger($this->_cur, $column);
            case self::BIND_OUTPUT_TYPE_STRING:
                $this->_outputBindVars[$column] = array('type'=>$type, 'var'=> &$variable);
                return sqlrcur_defineOutputBindString($this->_cur, $column, $length==null ? 2048 : $length );
            case self::BIND_OUTPUT_TYPE_BLOB:
                $this->_outputBindVars[$column] = array('type'=>$type, 'var'=> &$variable);
                return sqlrcur_defineOutputBindBlob($this->_cur, $column );
            case self::BIND_OUTPUT_TYPE_CLOB:
                $this->_outputBindVars[$column] = array('type'=>$type, 'var'=> &$variable);
                return sqlrcur_defineOutputBindClob($this->_cur, $column );
            default:
                return sqlrcur_inputBind($this->_cur, $column, $variable);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function closeCursor()
    {
        sqlrcur_clearBinds();
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function columnCount()
    {
        return sqlrcur_colCount($this->_cur);
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
        return sqlrcur_errorMessage($this->_cur);
    }

    /**
     * {@inheritdoc}
     */
    public function execute($params = null)
    {
        if ($params) {
            $hasZeroIndex = array_key_exists(0, $params);
            foreach ($params as $key => $val) {
                if ($hasZeroIndex && is_numeric($key)) {
                    $this->bindValue($key + 1, $val);
                } else {
                    $this->bindValue($key, $val);
                }
            }
        }
        $ret = sqlrcur_executeQuery($this->_cur);
        foreach ($this->_outputBindVars as $key => $var) {
            switch ($this->_outputBindVars[$key]['type']) {
                case self::BIND_OUTPUT_TYPE_CURSOR:
                    $curbind=sqlrcur_getOutputBindCursor($this->_cur,$key);
                    sqlrcur_fetchFromBindCursor($curbind);
                    $this->_outputBindVars[$key]['var'] = new Statement($this->_dbh, $key, $this->_conn, $curbind);
                    break;
                case self::BIND_OUTPUT_TYPE_DOUBLE:
                    $result=sqlrcur_getOutputBindDouble($this->_cur, $key);
                    $this->_outputBindVars[$key]['var'] = $result;
                    break;
                case self::BIND_OUTPUT_TYPE_INTEGER:
                    $result=sqlrcur_getOutputBindInteger($this->_cur, $key);
                    $this->_outputBindVars[$key]['var'] = $result;
                    break;
                case self::BIND_OUTPUT_TYPE_STRING:
                    $result=sqlrcur_getOutputBindString($this->_cur, $key);
                    $this->_outputBindVars[$key]['var'] = $result;
                    break;
                case self::BIND_OUTPUT_TYPE_BLOB:
                    $result=sqlrcur_getOutputBindBlob($this->_cur, $key);
                    $this->_outputBindVars[$key]['var'] = $result;
                    break;
                case self::BIND_OUTPUT_TYPE_CLOB:
                    $result=sqlrcur_getOutputBindClob($this->_cur, $key);
                    $this->_outputBindVars[$key]['var'] = $result;
                    break;
                default:
                    throw new \Exception('output_binds_not_defined_correctly');
            }
            
            //$this->_outputBindVars[$key] = 5;
        }
        
        
         /*for ($i=0; $i<sqlrcur_rowCount($bindcur); $i++) {
                for ($j=0; $j<sqlrcur_colCount($bindcur); $j++) {
                        echo sqlrcur_getField($bindcur,$i,$j);
                        echo ", ";
                }
                echo "\n";
        }*/
        
        if ( ! $ret) {
            throw Exception::fromErrorInfo($this->errorInfo());
        }
        $this->_currentRow = 0;
        return $ret;
    }

    /**
     * {@inheritdoc}
     */
    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null)
    {
        $this->_defaultFetchMode = $fetchMode;

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function getIterator()
    {
        $data = $this->fetchAll();

        return new \ArrayIterator($data);
    }

    /**
     * {@inheritdoc}
     */
    public function fetch($fetchMode = null)
    {
        $fetchMode = $fetchMode ?: $this->_defaultFetchMode;
        if ( ! isset(self::$fetchModeMap[$fetchMode])) {
            throw new \InvalidArgumentException("Invalid fetch style: " . $fetchMode);
        }
        return sqlrcur_getRowAssoc($this->_cur, $this->_currentRow++);
        //return oci_fetch_array($this->_sth, self::$fetchModeMap[$fetchMode] | OCI_RETURN_NULLS | OCI_RETURN_LOBS);
    }

    /**
     * {@inheritdoc}
     */
    public function fetchAll($fetchMode = null)
    {
        $fetchMode = $fetchMode ?: $this->_defaultFetchMode;
        if ( ! isset(self::$fetchModeMap[$fetchMode])) {
            throw new \InvalidArgumentException("Invalid fetch style: " . $fetchMode);
        }

        $result = array();
        if (true || self::$fetchModeMap[$fetchMode] === OCI_BOTH) {
            while ($row = $this->fetch($fetchMode)) {
                $result[] = $row;
            }
        } else {
            $fetchStructure = OCI_FETCHSTATEMENT_BY_ROW;
            if ($fetchMode == PDO::FETCH_COLUMN) {
                $fetchStructure = OCI_FETCHSTATEMENT_BY_COLUMN;
            }

            oci_fetch_all($this->_sth, $result, 0, -1,
                    self::$fetchModeMap[$fetchMode] | OCI_RETURN_NULLS | $fetchStructure | OCI_RETURN_LOBS);

            if ($fetchMode == PDO::FETCH_COLUMN) {
                $result = $result[0];
            }
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function fetchColumn($columnIndex = 0)
    {
        $row = sqlrcur_getRowAssoc($this->_cur, $this->_currentRow++);

        return isset($row[$columnIndex]) ? $row[$columnIndex] : false;
    }

    /**
     * {@inheritdoc}
     */
    public function rowCount()
    {
        return sqlrcur_totalRows($this->_cur);
    }
}
