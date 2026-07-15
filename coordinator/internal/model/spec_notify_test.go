package model

import "testing"

const notifyBase = `apiVersion: drsync/v1
kind: Job
metadata:
  name: n
spec:
  source: { path: /src }
  destination: { path: /dst }
`

func TestNotificationsParse(t *testing.T) {
	spec := notifyBase + `  notifications:
    recipients: [ops@example.com, lead@example.com]
    on_pass_complete: true
    on_job_complete: true
`
	s, err := ParseSpec([]byte(spec))
	if err != nil {
		t.Fatal(err)
	}
	n := s.Spec.Notifications
	if len(n.Recipients) != 2 || n.Recipients[0] != "ops@example.com" {
		t.Fatalf("recipients = %v", n.Recipients)
	}
	if !n.OnPassComplete || !n.OnJobComplete || !n.Enabled() {
		t.Fatalf("flags not parsed: %+v", n)
	}
}

func TestNotificationsAbsentIsDisabled(t *testing.T) {
	s, err := ParseSpec([]byte(notifyBase))
	if err != nil {
		t.Fatal(err)
	}
	if s.Spec.Notifications.Enabled() {
		t.Fatal("absent notifications block should be disabled")
	}
}

func TestNotificationsRequireRecipients(t *testing.T) {
	spec := notifyBase + `  notifications:
    on_pass_complete: true
`
	if _, err := ParseSpec([]byte(spec)); err == nil {
		t.Fatal("enabling notifications without recipients should fail validation")
	}
}

func TestNotificationsRejectBadAddress(t *testing.T) {
	spec := notifyBase + `  notifications:
    recipients: [not-an-email]
    on_job_complete: true
`
	if _, err := ParseSpec([]byte(spec)); err == nil {
		t.Fatal("invalid recipient address should fail validation")
	}
}
