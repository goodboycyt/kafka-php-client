<?php
namespace KafkaPhp;

require 'KafkaException.php';

/**
 * Class KafkaProducer
 *
 * Kafka Services client class, which wraps all frequently used APIs user could call to talk to kafka.
 * Users could do operations like creating a producer,producing messages,height-level consuming,low-level consuming via an KafkaClient instance.
 * For more details, please check out the Kafka API document:https://github.com/goodboycyt/kafka-php-client
 * @package Kafka
 */
class KafkaProducer
{
    /**
     * Constructor
     *
     * Nothing to say.
     *
     * @param string $host The host of Kafka server with port like 193.23.1.1:9093
//     * @param integer $qBufferMaxTime Max time(ms) for push msg.
     * @throws KafkaException
     */
    public function __construct($host)
    {
        $host = trim($host);
        if (empty($host)) {
            throw new KafkaException(['code'=>28,'message'=>'host is empty']);
        }
        $conf = new \RdKafka\Conf();
        $conf->set('api.version.request', 'true');
        $conf->set('message.send.max.retries', 1);
        $conf->set('api.version.request.timeout.ms', 1000);
        $conf->set('queue.buffering.max.ms', 1);
        $this->producer = new \RdKafka\Producer($conf);
        $this->producer->addBrokers($host);
    }

    /**
     * Producer a message
     *
     * Nothing to say.
     *
     * @param string $msg msg body
     * @param string $topic which topic to push msg.
     * @param integer $part which partition
     * @param integer $flushTime flush time before you destory producer instance
     * @throws KafkaException
     */
    public function sendMsg($topic, $msg, $flushTime = 10, $part = 0)
    {
        if (empty($msg) || empty($topic) || $this->producer==null) {
            throw new KafkaException(['code'=>48,'message'=>'topic or msg or producer is empty']);
        }
        $this->topic = $this->producer->newTopic($topic);
        $this->topic->produce(RD_KAFKA_PARTITION_UA, 0, $msg);
        usleep(1000);
        $this->producer->purge(RD_KAFKA_PURGE_F_QUEUE);
        $this->producer->flush(20);
    }

    public function __destruct()
    {
    }

    private $producer;
    private $topic;
}