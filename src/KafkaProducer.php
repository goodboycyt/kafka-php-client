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
        $conf->set('socket.timeout.ms', 10);
        $conf->setDrmSgCb(function ($kafka, $message ) {
            $this->sendReport = $message->err;
        });
        $conf->setErrorCb(function ($kafka, $err, $reason) {
            $this->sendReport = $reason;
        });
        $conf->set('api.version.request', 'true');
        $conf->set('message.send.max.retries', 2);
        $conf->set('api.version.request.timeout.ms', 10000);
        $conf->set('queue.buffering.max.ms', 1);
        $conf->set('bootstrap.servers', $host);
        $conf->set('topic.metadata.refresh.sparse', true);//仅获取自己用的元数据 减少带宽
        $conf->set('topic.metadata.refresh.interval.ms', 600000);//设置刷新元数据时间间隔为600s 减少带宽
        $conf->set('log.connection.close', 'false');
        if (function_exists('pcntl_sigprocmask')) {
            pcntl_sigprocmask(SIG_BLOCK, array(SIGIO));
            $conf->set('internal.termination.signal', SIGIO);//设置kafka客户端线程在其完成后立即终止
        }
        $this->producer = new \RdKafka\Producer($conf);
        $this->producer->addBrokers($host);
    }

    /**
     * Producer a message
     *
     * Nothing to say.
     *
     * @param string $topic which topic to push msg.
     * @param string $msg msg body
     * @param bool $sync
     * @param integer $flushTime flush time before you destory producer instance
     * @param integer $part which partition
     * @throws KafkaException
     */
    public function sendMsg($topic, $msg,$key=null, $sync = false, $flushTime = 10, $part = 0)
    {
        if (empty($msg) || empty($topic) || $this->producer==null) {
            throw new KafkaException(['code'=>48,'message'=>'topic or msg or producer is empty']);
        }
        $cf = new \RdKafka\TopicConf();
        $cf->set('request.required.acks', 1);
        $cf->set('request.timeout.ms', 5000);
        $cf->set('message.timeout.ms', 5000);
        $this->topic = $this->producer->newTopic($topic, $cf);
        $this->topic->produce(RD_KAFKA_PARTITION_UA, 0, $msg,$key);
        if (!$sync) {
            do {
                $this->producer->poll(1);
            } while ($this->producer->getOutQLen() > 0);
        }
    }

    public function __destruct()
    {
        while ($this->producer->getOutQLen() > 0) {
            $this->producer->poll(1);
        }
    }

    private $producer;
    private $topic;
    public $sendReport;
}