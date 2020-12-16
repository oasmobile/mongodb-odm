<?php

namespace Oasis\Mlib\ODM\MongoDB\Driver;


use MongoDB\Client;

class MongoDBTable
{
    public function __construct()
    {
        $client = new Client("mongodb://127.0.0.1/");
        $client->selectDatabase("database-name")->selectCollection("table-name");
    }

}
