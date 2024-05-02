package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/dyammarcano/kv"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

const storePrefix = "store_cliApp"

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "kv",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: restCmd,
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().BoolP("rest", "r", false, "Enable REST API")
	rootCmd.Flags().IntP("port", "p", 4400, "Port for the REST API")
	rootCmd.Flags().StringP("datastore", "d", "badger", "Path to the datastore")
}

func restCmd(cmd *cobra.Command, args []string) error {
	restFlag, err := cmd.Flags().GetBool("rest")
	if err != nil {
		return fmt.Errorf("error getting rest flag: %w", err)
	}

	if restFlag {
		portInt, err := cmd.Flags().GetInt("port")
		if err != nil {
			return fmt.Errorf("error getting port flag: %w", err)
		}

		portStr, err := checkPort(portInt)
		if err != nil {
			return err
		}

		datastore, err := cmd.Flags().GetString("datastore")
		if err != nil {
			return fmt.Errorf("error getting datastore flag: %w", err)
		}

		datastoreStr := filepath.Clean(datastore)

		if _, err = os.Stat(datastore); os.IsNotExist(err) {
			if err := os.MkdirAll(datastore, os.ModePerm); err != nil {
				return fmt.Errorf("error creating datastore path: %w", err)
			}
		}

		// Start REST API
		return restServer(cmd.Context(), datastoreStr, portStr)
	}

	return nil
}

func restServer(ctx context.Context, datastore, port string) error {
	cfg := &kv.Config{
		StorePath: datastore,
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("error validating config: %w", err)
	}

	newKV, err := kv.NewKV(ctx, cfg)
	if err != nil {
		return fmt.Errorf("error creating new store: %w", err)
	}

	defer func(newKV *kv.KV) {
		if err = newKV.Close(); err != nil {
			fmt.Println("error closing store: %w", err)
		}
	}(newKV)

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	// r.Group("/api/v1",func(r chi.Router) {
	//	r.Use(middleware.RequestID)
	//	r.Use(middleware.RealIP)
	//	r.Use(middleware.Logger)
	//	r.Use(middleware.Recoverer)
	//})

	r.Group(func(r chi.Router) {
		r.Get("/keys/{key}", func(w http.ResponseWriter, r *http.Request) {
			key := chi.URLParam(r, "key")
			keys, err := newKV.Keys(newKV.MakeKeyStr(storePrefix, key))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Write([]byte(fmt.Sprintf("Keys: %v", keys)))
		})

		r.Get("/key/{key}", func(w http.ResponseWriter, r *http.Request) {
			key := chi.URLParam(r, "key")
			value, err := newKV.Get(newKV.MakeKeyStr(storePrefix, key))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			data, err := base64.StdEncoding.DecodeString(string(value))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Write(data)
		})

		r.Post("/key/{key}", func(w http.ResponseWriter, r *http.Request) {
			key := chi.URLParam(r, "key")
			value := r.FormValue("value")
			ttl := r.FormValue("ttl")
			data := base64.StdEncoding.EncodeToString([]byte(value))
			if data == "" {
				http.Error(w, "value is empty", http.StatusBadRequest)
				return
			}
			if err := r.ParseForm(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if ttl == "" {
				if err := newKV.Set(newKV.MakeKeyStr(storePrefix, key), []byte(data)); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				w.Write([]byte(fmt.Sprintf("data stored, key: %s", newKV.MakeKeyStr(storePrefix, key))))
			}

			ttlTime, err := time.ParseDuration(ttl)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if err := newKV.SetExpire(newKV.MakeKeyStr(storePrefix, key), []byte(data), ttlTime); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Write([]byte(fmt.Sprintf("data stored, key: %s", newKV.MakeKeyStr(storePrefix, key))))
		})
	})

	//r.Group(func(r chi.Router) {
	//	r.Use(authMiddleware)
	//	//r.Post("/manage", CreateAsset)
	//})

	fmt.Printf("Starting server on port %s\n", port)

	return http.ListenAndServe(port, r)
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Do some auth stuff
		next.ServeHTTP(w, r)
	})
}

func checkPort(port int) (string, error) {
	if port < 1024 || port > 65535 {
		return "", fmt.Errorf("invalid port number")
	}

	return fmt.Sprintf(":%d", port), nil
}
