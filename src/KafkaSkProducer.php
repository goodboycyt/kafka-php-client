<?php
namespace KafkaPhp;

require 'KafkaException.php';

/**
 * Class KafkaSkProducer
 *
 * special use this to send msg to pass system by socket,then pass system transmit msg to Kafka Services.
 * Users could do operations like producing messages via an KafkaSkProducer instance.
 * For more details, please check out the Kafka API document:https://github.com/goodboycyt/kafka-php-client
 * @package Kafka
 * @version 1.0
 * @author linwanfeng
 */
class KafkaSkProducer
{
    /**
     * Constructor
     *
     * Nothing to say.
     *
     * @param string $host The host of Kafka server with port like 193.23.1.1:9093
     * @param int $port 端口
     * @throws KafkaException
     */
    public function __construct($host, $port)
    {
        $host = trim($host);
        if (empty($host)) {
            throw new KafkaException(['code'=>30,'message'=>'host is empty']);
        }
        $this->socket = \socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        \socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, array("sec" => 1, "usec" => 10000));
        \socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, array("sec" => 1, "usec" => 10000));
        if (\socket_connect($this->socket, $host, $port) == false) {
            \socket_close($this->socket);//工作完毕，关闭套接流
            throw new KafkaException(['code'=>37,'message'=>'host connect fail']);
        }
    }

    /**
     * Producer a message
     *
     * Nothing to say.
     *
     * @param string $msg msg body
     * @param string $topic which topic to push msg.
     * @return false|string
     * @throws KafkaException
     */
    public function sendMsg($topic, $msg)
    {
        if (empty($msg) || empty($topic) || $this->socket==null) {
            throw new KafkaException(['code'=>55,'message'=>'topic or msg or socket is empty']);
        }
        $data['topic'] = $topic;
        $data['msg'] = $msg;
        $message = json_encode($data);
        $message = mb_convert_encoding($message, 'GBK', 'UTF-8');
        if (\socket_write($this->socket, $message, strlen($message)) == false) {
            \socket_close($this->socket);//工作完毕，关闭套接流
            throw new KafkaException(['code'=>63,'message'=>'socket 写入错误']);
        } else {
            while ($callback = \socket_read($this->socket, 2048)) {
                return $callback;
                break;
            }
        }
    }

    public function __destruct()
    {
        socket_close($this->socket);//工作完毕，关闭套接流
    }

    private $socket;
}