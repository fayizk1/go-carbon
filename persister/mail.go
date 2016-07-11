package persister

import (
	"github.com/Sirupsen/logrus"
	"github.com/go-gomail/gomail"
)

func sendMail(fromID string, toIDs []string, server , username, password, subject, body string) {
	m := gomail.NewMessage()
	m.SetAddressHeader("From", fromID, "")
	var ids []string
	for i := range toIDs {
		ids = append(ids, m.FormatAddress(toIDs[i], ""))
	}
	m.SetHeader("To",
		ids...
	)
	m.SetHeader("Subject", subject)
	m.SetBody("text/plain", body)
	
	d := gomail.NewPlainDialer(server, 25, username, password)
	if err := d.DialAndSend(m); err != nil {
		logrus.Println("Unable to send mail", err)
	}
}
