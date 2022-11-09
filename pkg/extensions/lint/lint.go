//go:build lint
// +build lint

package lint

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	_ "image/gif"  //nolint:gci  // imported for the registration of it's decoder func
	_ "image/jpeg" //nolint:gci // imported for the registration of it's decoder func
	_ "image/png"  //nolint:gci  // imported for the registration of it's decoder func

	godigest "github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"

	"zotregistry.io/zot/pkg/extensions/config"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/storage"
)

const (
	logoKey string = "com.zot.logo"
)

type Linter struct {
	config *config.LintConfig
	log    log.Logger
}

func NewLinter(config *config.LintConfig, log log.Logger) *Linter {
	return &Linter{
		config: config,
		log:    log,
	}
}

func (linter *Linter) CheckMandatoryAnnotations(repo string, manifestDigest godigest.Digest,
	imgStore storage.ImageStore,
) (bool, error) {
	if linter.config == nil {
		return true, nil
	}

	if (linter.config != nil && !*linter.config.Enable) || len(linter.config.MandatoryAnnotations) == 0 {
		return true, nil
	}

	mandatoryAnnotationsList := linter.config.MandatoryAnnotations

	content, err := imgStore.GetBlobContent(repo, manifestDigest)
	if err != nil {
		linter.log.Error().Err(err).Msg("linter: unable to get image manifest")

		return false, err
	}

	var manifest ispec.Manifest

	if err := json.Unmarshal(content, &manifest); err != nil {
		linter.log.Error().Err(err).Msg("linter: couldn't unmarshal manifest JSON")

		return false, err
	}

	mandatoryAnnotationsMap := make(map[string]bool)
	for _, annotation := range mandatoryAnnotationsList {
		mandatoryAnnotationsMap[annotation] = false
	}

	manifestAnnotations := manifest.Annotations
	for annotation := range manifestAnnotations {
		if _, ok := mandatoryAnnotationsMap[annotation]; ok {
			if annotation == logoKey {
				if ok := linter.checkIfLogoAnnotationValid(manifestAnnotations[annotation]); ok {
					mandatoryAnnotationsMap[annotation] = true
				}

				continue
			}
			mandatoryAnnotationsMap[annotation] = true
		}
	}

	missingAnnotations := getMissingAnnotations(mandatoryAnnotationsMap)
	if len(missingAnnotations) == 0 {
		return true, nil
	}

	// if there are mandatory annotations missing in the manifest, get config and check these annotations too
	configDigest := manifest.Config.Digest

	content, err = imgStore.GetBlobContent(repo, configDigest)
	if err != nil {
		linter.log.Error().Err(err).Msg("linter: couldn't get config JSON " + configDigest.String())

		return false, err
	}

	var imageConfig ispec.Image
	if err := json.Unmarshal(content, &imageConfig); err != nil {
		linter.log.Error().Err(err).Msg("linter: couldn't unmarshal config JSON " + configDigest.String())

		return false, err
	}

	configAnnotations := imageConfig.Config.Labels
	for annotation := range configAnnotations {
		if _, ok := mandatoryAnnotationsMap[annotation]; ok {
			if annotation == logoKey {
				if ok := linter.checkIfLogoAnnotationValid(configAnnotations[annotation]); ok {
					mandatoryAnnotationsMap[annotation] = true
				}

				continue
			}
			mandatoryAnnotationsMap[annotation] = true
		}
	}

	missingAnnotations = getMissingAnnotations(mandatoryAnnotationsMap)
	if len(missingAnnotations) > 0 {
		linter.log.Error().Msgf("linter: manifest %s / config %s are missing annotations: %s",
			string(manifestDigest), string(configDigest), missingAnnotations)

		return false, nil
	}

	return true, nil
}

func (linter *Linter) Lint(repo string, manifestDigest godigest.Digest,
	imageStore storage.ImageStore,
) (bool, error) {
	return linter.CheckMandatoryAnnotations(repo, manifestDigest, imageStore)
}

func getMissingAnnotations(mandatoryAnnotationsMap map[string]bool) []string {
	var missingAnnotations []string

	for annotation, flag := range mandatoryAnnotationsMap {
		if !flag {
			missingAnnotations = append(missingAnnotations, annotation)
		}
	}

	return missingAnnotations
}

func (linter *Linter) checkIfLogoAnnotationValid(annotationVal string) bool {
	decodedVal, err := base64.StdEncoding.DecodeString(annotationVal)
	if err != nil {
		linter.log.Error().Err(err).Msg("unable to decode value")

		return false
	}
	imageVal := bytes.NewBuffer(decodedVal)

	logoConf, format, err := image.DecodeConfig(imageVal)
	if err != nil {
		linter.log.Error().Err(err).Msg("unable to decode image")

		return false
	}

	if format != "jpeg" && format != "gif" && format != "png" {
		linter.log.Error().Msg("encoded logo is of incorrect format, allowed formats are jpeg/png/gif")

		return false
	}

	if logoConf.Height > 200 || logoConf.Width > 200 {
		linter.log.Error().Msg("encoded logo is of incorrect size")

		return false
	}

	return true
}
