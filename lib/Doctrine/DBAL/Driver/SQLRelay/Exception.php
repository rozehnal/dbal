<?php

namespace Doctrine\DBAL\Driver\SQLRelay;

/**
 * @author Pavel Rozehnal <pavel.rozehnal@dixonsretail.com>
 */
class Exception extends \Exception
{
    /**
     * @param array $error
     *
     * @return \Doctrine\DBAL\Driver\SQLRelay\Exception
     */
    static public function fromErrorInfo($error)
    {
        return new self($error['message']);
    }
}
