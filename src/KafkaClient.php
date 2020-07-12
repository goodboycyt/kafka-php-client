<?php
namespace KafkaPhp;

require 'KafkaException.php';

/**
 * Class KafkaClient
 *
 * Kafka Services client class, which wraps all frequently used APIs user could call to talk to kafka.
 * Users could do operations like creating a producer,producing messages,height-level consuming,low-level consuming via an KafkaClient instance.
 * For more details, please check out the Kafka API document:https://github.com/goodboycyt/kafka-php-client
 * @package Kafka
 */
class KafkaClient
{
    /**
     * Constructor
     *
     * Nothing to say.
     *
     * @param string $host The host of Kafka server with port like 193.23.1.1:9093
     * @param integer $versionTimeOut Max time(ms) for get api version.
     * @throws KafkaException
     */
    public function __construct($host, $versionTimeOut)
    {
        $host = trim($host);
        if (empty($host) ) {
            throw new KafkaException(['code'=>28,'message'=>'host is empty']);
        }
        $this->host = $host;
        $this->versionTimeOut = $versionTimeOut;
    }

    /**
     * Producer a message
     *
     * Nothing to say.
     *
     * @param string $topic which topic to send
     * @param string $msg msg body
     * @param integer $part which partition
     * @param integer $flushTime flush time before you destory producer instance
     * @throws KafkaException
     */
    public function sendMsg($topic, $msg, $flushTime = 10, $part = 0)
    {
        if (empty($topic) || empty($msg) || self::$producer==null) {
            throw new KafkaException(['code'=>48,'message'=>'topic or msg or producer is empty']);
        }
        $topic = self::$producer->newTopic($topic);
        $topic->produce(RD_KAFKA_PARTITION_UA, $part, $msg);
        self::$producer->flush($flushTime);

    }

    /**
     * create a Producer
     *
     * Nothing to say.
     *
     * @param integer $queueBufferMaxTime Max time(ms) for queue buffer wait.
     * @throws KafkaException
     * @return object RdKafka\Producer
     */
    public function initProducer($queueBufferMaxTime)
    {
        if (!is_numeric($queueBufferMaxTime)) {
            throw new KafkaException(['code'=>115,'message'=>'queueBufferMaxTime is empty']);
        }
        $conf = new \RdKafka\Conf();
        $conf->set('api.version.request', 'true');
        $conf->set('message.send.max.retries', 1);
        $conf->set('api.version.request.timeout.ms', $this->versionTimeOut);
        $conf->set('queue.buffering.max.ms', $queueBufferMaxTime);
        $conf->set('bootstrap.servers', $this->host);
        self::$producer = new \RdKafka\Producer($conf);
    }
    /**
     * create a Consumer
     *
     * Nothing to say.
     *
     * @param array $topic topics which you subscribe
     * @param int $groupId consumer groupid
     * @throws KafkaException
     */
    public function initConsumer($topic, $groupId)
    {
        if (!is_numeric($groupId) || empty($topic)) {
            throw new KafkaException(['code'=>138,'message'=>'topic or groupId is empty']);
        }
        $conf = new \RdKafka\Conf();
        $conf->set('api.version.request', 'true');
        $conf->set('api.version.request.timeout.ms', $this->versionTimeOut);
//        $conf->set('queue.buffering.max.ms', 1);
        $conf->set('group.id', $groupId);
        $conf->set('bootstrap.servers', $this->host);
        self::$consumer = new \RdKafka\KafkaConsumer($conf);
        self::$consumer->subscribe($topic);
    }
    /**
     * get some messages
     *
     * Nothing to say.
     *
     * @param integer $num how much msg you want get
     * @param integer $timeout read timeout
     * @throws KafkaException
     * @return array
     */
    public function getMsg($num, $timeout = 10)
    {
        if (!is_numeric($num) || self::$consumer==null) {
            throw new KafkaException(['code'=>115,'message'=>'num or consumer is empty']);
        }
        $msgs = [];
        for ($i = 0; $i < $num; $i++) {
            $message = self::$consumer->consume($timeout);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $msgs[] = ['t_name'=>$message->topic_name,'msg'=>$message->payload, 'off'=>$message->offset];
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    return $msgs;
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    return $msgs;
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
        return $msgs;

    }



    private $host;
    private $versionTimeOut;
    private static $producer;
    private static $consumer;
}