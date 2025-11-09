<?php

use Tests\TestCase;

final class SqlTest extends TestCase
{
    public function testResult()
    {
        $this->createEvents();
        $result = $this->client->sql('SELECT * FROM events');
        $this->assertEquals(['a', 'b'], $result->columns());
        $this->assertEquals([[1, 'one'], [2, 'two'], [3, 'three']], $result->rows());
        $this->assertEquals([['a' => 1, 'b' => 'one'], ['a' => 2, 'b' => 'two'], ['a' => 3, 'b' => 'three']], $result->toArray());
    }

    public function testTypes()
    {
        $this->assertIsInt($this->client->sql('SELECT 1')->rows()[0][0]);
        $this->assertIsFloat($this->client->sql('SELECT 1.0')->rows()[0][0]);
        $this->assertInstanceOf(Saturio\DuckDB\Type\Date::class, $this->client->sql('SELECT current_date')->rows()[0][0]);
        $this->assertInstanceOf(Saturio\DuckDB\Type\Time::class, $this->client->sql('SELECT current_time')->rows()[0][0]);
        $this->assertTrue($this->client->sql('SELECT true')->rows()[0][0]);
        $this->assertFalse($this->client->sql('SELECT false')->rows()[0][0]);
        $this->assertNull($this->client->sql('SELECT NULL')->rows()[0][0]);
    }

    public function testParams()
    {
        $this->assertIsInt($this->client->sql('SELECT ?', [1])->rows()[0][0]);
        $this->assertIsFloat($this->client->sql('SELECT ?', [1.0])->rows()[0][0]);
        $this->assertTrue($this->client->sql('SELECT ?', [true])->rows()[0][0]);
        $this->assertFalse($this->client->sql('SELECT ?', [false])->rows()[0][0]);
        $this->assertNull($this->client->sql('SELECT ?', [null])->rows()[0][0]);
        $this->assertInstanceOf(Saturio\DuckDB\Type\Timestamp::class, $this->client->sql('SELECT ?', [new DateTime()])->rows()[0][0]);
    }

    public function testExtraParams()
    {
        $this->expectException(Saturio\DuckDB\Exception\BindValueException::class);
        $this->expectExceptionMessage("Couldn't bind parameter '2' to prepared statement");

        $this->client->sql('SELECT ?', [1, 2]);
    }

    public function testUpdate()
    {
        $this->createEvents();
        $this->client->sql('UPDATE events SET b = ? WHERE a = ?', ['two!', 2]);
        $result = $this->client->sql('SELECT * FROM events ORDER BY a');
        $this->assertEquals([[1, 'one'], [2, 'two!'], [3, 'three']], $result->rows());
    }

    public function testDelete()
    {
        $this->createEvents();
        $this->client->sql('DELETE FROM events WHERE a = ?', [2]);
        $result = $this->client->sql('SELECT * FROM events ORDER BY a');
        $this->assertEquals([[1, 'one'], [3, 'three']], $result->rows());
    }

    public function testView()
    {
        try {
            $this->createEvents();
            $this->client->sql('CREATE VIEW events_view AS SELECT a AS c, b AS d FROM events');
            $result = $this->client->sql('SELECT * FROM events_view');
            $this->assertEquals(['c', 'd'], $result->columns());
            $this->assertEquals([[1, 'one'], [2, 'two'], [3, 'three']], $result->rows());
        } finally {
            $this->client->sql('DROP VIEW IF EXISTS events_view');
        }
    }

    public function testPartitioning()
    {
        $this->client->sql('CREATE TABLE events (a bigint, b text)');
        $this->client->sql('ALTER TABLE events SET PARTITIONED BY (a)');
        $this->loadEvents();
        $this->assertCount(3, $this->client->listFiles('events'));

        $this->client->sql('ALTER TABLE events RESET PARTITIONED BY');
        $this->loadEvents();
        $this->assertCount(4, $this->client->listFiles('events'));
    }

    public function testMultipleStatements()
    {
        // TODO fix
        $this->expectException(TypeError::class);

        $this->client->sql('SELECT 1; SELECT 2');
    }

    public function testQuoteIdentifier()
    {
        $this->assertEquals('"events"', $this->client->quoteIdentifier('events'));
        $this->assertEquals('"""events"""', $this->client->quoteIdentifier('"events"'));
    }

    public function testQuote()
    {
        $this->assertEquals('NULL', $this->client->quote(null));
        $this->assertEquals('true', $this->client->quote(true));
        $this->assertEquals('false', $this->client->quote(false));
        $this->assertEquals('1', $this->client->quote(1));
        $this->assertEquals('0.5', $this->client->quote(0.5));
        $this->assertEquals("'2025-01-02T03:04:05.123456Z'", $this->client->quote(new DateTime('2025-01-02 03:04:05.123456')));
        $this->assertEquals("'hello'", $this->client->quote('hello'));
    }

    public function testQuoteUnsupportedType()
    {
        $this->expectException(TypeError::class);
        $this->expectExceptionMessage("can't quote");

        $this->client->quote((object) 1);
    }

    public function testQuoteStatement()
    {
        $value = join(array_map(fn ($i) => array_rand(array_flip(['a', "'", '"', '\\'])), range(1, 19)));
        $this->assertEquals($value, $this->client->sql('SELECT ' . $this->client->quote($value) . ' AS value')->rows()[0][0]);
    }
}
