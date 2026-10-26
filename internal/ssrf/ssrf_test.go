package ssrf

import "testing"

// TestCheckEndpointURL is FR-402 and AC-19 at the URL level: https-only in live,
// private/metadata ranges rejected, and the internal flag as the only escape.
func TestCheckEndpointURL(t *testing.T) {
	t.Run("live requires https", func(t *testing.T) {
		if err := CheckEndpointURL("http://example.com/hook", "live", false); err == nil {
			t.Fatal("http in live was allowed; FR-402 requires https")
		}
		if err := CheckEndpointURL("https://example.com/hook", "live", false); err != nil {
			t.Fatalf("https in live rejected: %v", err)
		}
		if err := CheckEndpointURL("ftp://example.com/hook", "live", false); err == nil {
			t.Fatal("ftp scheme was allowed")
		}
	})

	t.Run("test allows http", func(t *testing.T) {
		if err := CheckEndpointURL("http://example.com/hook", "test", false); err != nil {
			t.Fatalf("http in test rejected: %v", err)
		}
	})

	t.Run("metadata and private ranges rejected (AC-19)", func(t *testing.T) {
		for _, u := range []string{
			"http://169.254.169.254/latest/meta-data/", // AC-19
			"https://169.254.169.254/",
			"http://127.0.0.1/",
			"http://localhost/",
			"http://10.0.0.5/",
			"http://192.168.1.20/",
			"http://172.16.5.4/",
			"http://0.0.0.0/",
		} {
			if err := CheckEndpointURL(u, "test", false); err == nil {
				t.Fatalf("internal address %s was allowed", u)
			}
		}
	})

	t.Run("global unicast allowed", func(t *testing.T) {
		// 8.8.8.8 is a public anycast address; the literal avoids DNS in the test.
		if err := CheckEndpointURL("https://8.8.8.8/hook", "live", false); err != nil {
			t.Fatalf("public address rejected: %v", err)
		}
	})

	t.Run("internal flag is the only escape", func(t *testing.T) {
		if err := CheckEndpointURL("http://10.0.0.5/hook", "test", true); err != nil {
			t.Fatalf("internal endpoint on a private range rejected: %v", err)
		}
		// The scheme rule still holds even for an internal endpoint.
		if err := CheckEndpointURL("http://10.0.0.5/hook", "live", true); err == nil {
			t.Fatal("internal flag let http through in live; scheme is independent of internal")
		}
	})

	t.Run("malformed and empty host", func(t *testing.T) {
		if err := CheckEndpointURL("not a url", "test", false); err == nil {
			t.Fatal("garbage was accepted")
		}
		if err := CheckEndpointURL("https://", "test", false); err == nil {
			t.Fatal("empty host was accepted")
		}
	})
}
