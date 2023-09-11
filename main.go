package main

import (
	"context"
	"github.com/gin-gonic/gin"
	_ "github.com/joho/godotenv/autoload"
	"github.com/segmentio/kafka-go"
	"log"
)

func main() {
	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	r.POST("/write", writeMsg)
	r.GET("/hi", func(c *gin.Context) {
		w, err := getDefaultKafkaWriter()
		if err != nil {
			log.Fatal(err)
		}

		err = w.WriteMessages(context.Background(), kafka.Message{Value: []byte("one!")})
		if err != nil {
			log.Println("error writing message:", err)
			c.JSON(500, gin.H{
				"message": "err",
				"data":    err.Error(),
			})
			return
		}
		err = w.Close()
		if err != nil {
			log.Println("error closing writer:", err)
		}
		c.JSON(200, gin.H{
			"message": "hi",
			"data":    "ok",
		})
	})
	r.Run() // 监听并在 0.0.0.0:8080 上启动服务
}

func writeMsg(c *gin.Context) {
	data, err := c.GetRawData()
	if err != nil {
		log.Println(err)
	}
	s := string(data)
	w, err := getDefaultKafkaWriter()
	if err != nil {
		log.Fatal(err)
	}

	err = w.WriteMessages(context.Background(), kafka.Message{Value: []byte(s)})
	if err != nil {
		log.Println("error writing message:", err)
	}
	err = w.Close()
	if err != nil {
		log.Println("error closing writer:", err)
	}
	c.JSON(200, gin.H{
		"message": s,
		"data":    "ok",
	})
}
