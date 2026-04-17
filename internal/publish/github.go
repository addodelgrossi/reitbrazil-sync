package publish

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/go-github/v66/github"
)

// GitHubOptions configures the monthly release uploader.
type GitHubOptions struct {
	Token  string
	Repo   string // "owner/name"
	Logger *slog.Logger
}

// GitHubPublisher cuts monthly releases with the SQLite asset.
type GitHubPublisher struct {
	client *github.Client
	owner  string
	repo   string
	log    *slog.Logger
}

// NewGitHubPublisher returns a publisher authenticated with token.
func NewGitHubPublisher(opts GitHubOptions) (*GitHubPublisher, error) {
	if opts.Token == "" {
		return nil, errors.New("publish: GitHub token is required")
	}
	if opts.Repo == "" {
		return nil, errors.New("publish: GitHub repo is required")
	}
	parts := strings.SplitN(opts.Repo, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("publish: invalid repo %q, expected owner/name", opts.Repo)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &GitHubPublisher{
		client: github.NewClient(nil).WithAuthToken(opts.Token),
		owner:  parts[0],
		repo:   parts[1],
		log:    opts.Logger.With("component", "publish/github"),
	}, nil
}

// ReleaseRequest is the input to PublishRelease.
type ReleaseRequest struct {
	Tag          string // e.g. data-v2026.04
	Name         string // e.g. "Data snapshot — April 2026"
	Body         string // markdown changelog
	DBPath       string // local SQLite path
	MetadataPath string // local metadata.json path
}

// PublishRelease creates tag, release, and uploads both assets. Safe to
// retry on failure: if a release with the tag already exists we return
// an error instead of overwriting, preserving the previous artifact.
func (p *GitHubPublisher) PublishRelease(ctx context.Context, req ReleaseRequest) error {
	if req.Tag == "" {
		return errors.New("publish: release tag required")
	}
	if req.Name == "" {
		req.Name = fmt.Sprintf("Data snapshot — %s", time.Now().UTC().Format("January 2006"))
	}
	if req.DBPath == "" {
		return errors.New("publish: release DBPath required")
	}

	draft := false
	prerelease := false
	rel, _, err := p.client.Repositories.CreateRelease(ctx, p.owner, p.repo, &github.RepositoryRelease{
		TagName:    &req.Tag,
		Name:       &req.Name,
		Body:       &req.Body,
		Draft:      &draft,
		Prerelease: &prerelease,
	})
	if err != nil {
		return fmt.Errorf("create release: %w", err)
	}

	if err := p.uploadAsset(ctx, *rel.ID, req.DBPath); err != nil {
		return fmt.Errorf("upload db asset: %w", err)
	}
	if req.MetadataPath != "" {
		if err := p.uploadAsset(ctx, *rel.ID, req.MetadataPath); err != nil {
			return fmt.Errorf("upload metadata asset: %w", err)
		}
	}
	p.log.InfoContext(ctx, "github release published",
		"tag", req.Tag, "name", req.Name, "owner", p.owner, "repo", p.repo)
	return nil
}

func (p *GitHubPublisher) uploadAsset(ctx context.Context, releaseID int64, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	name := info.Name()
	_, _, err = p.client.Repositories.UploadReleaseAsset(ctx, p.owner, p.repo, releaseID,
		&github.UploadOptions{Name: name}, f)
	return err
}
