#!/bin/bash

# Exit on any error, but handle errors gracefully
set -e

# Enable error trapping
trap 'handle_error $? $LINENO' ERR

# =============================================================================
# Configuration
# =============================================================================

# Project configuration
readonly APP_NAME="octopus"
readonly MAIN_DIR="./"
readonly OUTPUT_DIR="build"

# Build metadata
readonly BUILD_TIME="$(TZ='Asia/Shanghai' date +'%F %T %z')"
readonly GIT_AUTHOR="syscc"
readonly GIT_VERSION="$(git describe --tags --abbrev=0 2>/dev/null || echo 'dev')"
readonly COMMIT_ID="$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"

# Build flags
readonly LDFLAGS="-X 'github.com/bestruirui/octopus/internal/conf.Version=${GIT_VERSION}' \
                  -X 'github.com/bestruirui/octopus/internal/conf.BuildTime=${BUILD_TIME}' \
                  -X 'github.com/bestruirui/octopus/internal/conf.Author=${GIT_AUTHOR}' \
                  -X 'github.com/bestruirui/octopus/internal/conf.Commit=${COMMIT_ID}' \
                  -s -w"

# =============================================================================
# Utility Functions
# =============================================================================

log_info() {
    echo "ℹ️  $1"
}

log_success() {
    echo "✅ $1"
}

log_error() {
    echo "❌ $1" >&2
}

log_warning() {
    echo "⚠️  $1" >&2
}

log_step() {
    echo ""
    echo "🔧 $1"
    echo "────────────────────────────────────────"
}

# Error handling function
handle_error() {
    local exit_code=$1
    local line_number=$2
    log_error "Build failed at line ${line_number} with exit code ${exit_code}"
    log_error "Command that failed: $(sed -n "${line_number}p" "$0" | xargs)"
    log_error "Check the output above for more details"
    exit $exit_code
}

# =============================================================================
# Setup Functions
# =============================================================================
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

prepare_environment() {
    log_step "Preparing build environment"

    # Check and install required commands
    log_info "Checking required commands..."

    # Check Go
    if ! command_exists go; then
        log_error "Go is not installed. Please install Go from https://golang.org/dl/"
        return 1
    fi

    local go_version=$(go version 2>/dev/null | grep -o 'go[0-9]\+\.[0-9]\+' | head -1)
    log_success "Go version: $go_version"

    # Check Python
    if ! command_exists python3; then
        log_error "Python is not installed. Please install Python from https://www.python.org/downloads/"
        return 1
    fi

    local python_version=$(python3 --version 2>/dev/null)
    log_success "Python version: $python_version"

    # Check Node.js
    if ! command_exists node; then
        log_error "Node.js is not installed. Please install Node.js from https://nodejs.org/"
        return 1
    fi

    local node_version=$(node --version 2>/dev/null)
    log_success "Node.js version: $node_version"

    # Check pnpm
    if ! command_exists pnpm; then
        log_error "pnpm is not installed. Please install pnpm: npm install -g pnpm"
        return 1
    fi

    local pnpm_version=$(pnpm --version 2>/dev/null)
    log_success "pnpm version: $pnpm_version"

    # Check git
    if ! command_exists git; then
        log_error "git is not installed."
        return 1
    fi

    # Check curl
    if ! command_exists curl; then
        log_error "curl is not installed."
        return 1
    fi

    # Check unzip
    if ! command_exists unzip; then
        log_error "unzip is not installed."
        return 1
    fi

    # Check tar
    if ! command_exists tar; then
        log_error "tar is not installed."
        return 1
    fi

    # Check zip
    if ! command_exists zip; then
        log_error "zip is not installed."
        return 1
    fi

    # Check md5sum (or md5 on macOS)
    if ! command_exists md5sum && ! command_exists md5; then
        log_error "md5sum or md5 is not installed."
        return 1
    fi

    log_success "All required commands installed"

    # Create output directory and subdirectories
    log_info "Creating output directory structure: ${OUTPUT_DIR}"

    # Check if OUTPUT_DIR exists (including symlinks)
    if [ -e "${OUTPUT_DIR}" ]; then
        if [ -d "${OUTPUT_DIR}" ]; then
            log_success "Output directory already exists: ${OUTPUT_DIR}"
        else
            log_error "Output path exists but is not a directory: ${OUTPUT_DIR}"
            log_error "Path type: $(ls -la "${OUTPUT_DIR}" 2>/dev/null || echo 'Cannot determine type')"
            return 1
        fi
    else
        # Try to create the directory
        if ! mkdir -p "${OUTPUT_DIR}"; then
            log_error "Failed to create output directory: ${OUTPUT_DIR}"
            log_error "Current working directory: $(pwd)"
            log_error "Directory permissions: $(ls -la . 2>/dev/null || echo 'Cannot list directory')"
            return 1
        fi
        log_success "Created output directory: ${OUTPUT_DIR}"
    fi

    # Create subdirectories for organized output
    local subdirs=("bin" "docker" "archives")
    for subdir in "${subdirs[@]}"; do
        if ! mkdir -p "${OUTPUT_DIR}/${subdir}"; then
            log_error "Failed to create subdirectory: ${OUTPUT_DIR}/${subdir}"
            return 1
        fi
    done
    log_success "Created output subdirectories: bin, docker, archives"

    log_info "Tidying Go modules..."
    if ! go mod tidy >/dev/null 2>&1; then
        log_error "Failed to tidy Go modules"
        return 1
    fi

    log_success "Build environment ready"
}

# =============================================================================
# Build Functions
# =============================================================================

build_frontend() {
    log_step "Building frontend"

    local web_dir="web"

    # Check if web directory exists
    if [ ! -d "$web_dir" ]; then
        log_error "Web directory not found: $web_dir"
        log_error "Please run this script from the project root directory"
        return 1
    fi

    # Change to web directory
    cd "$web_dir" || return 1

    # Install dependencies
    log_info "Installing frontend dependencies..."
    if ! pnpm install; then
        log_error "Failed to install frontend dependencies"
        cd ..
        return 1
    fi
    log_success "Frontend dependencies installed"

    # Build the project
    log_info "Building frontend project..."
    if ! NEXT_PUBLIC_APP_VERSION="$GIT_VERSION" pnpm run build; then
        log_error "Failed to build frontend project"
        cd ..
        return 1
    fi
    log_success "Frontend build completed"

    # Return to original directory
    cd ..

    # Move out directory to static directory
    log_info "Moving frontend output to static directory..."
    
    # Remove old static/out if exists
    if [ -d "static/out" ]; then
        if ! rm -rf "static/out"; then
            log_error "Failed to remove old static/out directory"
            return 1
        fi
        log_info "Removed old static/out directory"
    fi
    
    # Move web/out to static/out
    if [ -d "${web_dir}/out" ]; then
        if ! mv "${web_dir}/out" "static/"; then
            log_error "Failed to move frontend output to static/out"
            return 1
        fi
        log_success "Moved frontend output to static/out"
    else
        log_error "Frontend output directory not found: ${web_dir}/out"
        return 1
    fi

    return 0
}

update_price() {
    log_step "Updating price"
    if ! python3 scripts/updatePrice.py; then
        log_error "Failed to update price"
        return 1
    fi
    log_success "Price updated"
}


get_go_arch() {
    case "$1" in
    "x86_64") echo "amd64" ;;
    "arm64") echo "arm64" ;;
    "x86") echo "386" ;;
    "armv7") echo "arm" ;;
    *)
        log_error "Unsupported architecture: $1"
        return 1
        ;;
    esac
}

build_standard() {
    local os="$1"
    local arch="$2"
    local go_arch

    if ! go_arch="$(get_go_arch "${arch}")"; then
        log_error "Failed to get Go architecture: ${arch}"
        return 1
    fi

    local output_file="${OUTPUT_DIR}/bin/${APP_NAME}-${os}-${arch}"

    log_info "Building ${os}/${arch}..."

    if ! GOOS="${os}" GOARCH="${go_arch}" CGO_ENABLED=0 \
        go build -o "${output_file}" -ldflags="${LDFLAGS}" -tags=jsoniter "${MAIN_DIR}" 2>&1; then
        log_error "Failed to build ${os}/${arch}"
        log_error "Build command: GOOS=${os} GOARCH=${go_arch} CGO_ENABLED=0 go build -o ${output_file} -ldflags=\"${LDFLAGS}\" -tags=jsoniter ${MAIN_DIR}"
        return 1
    fi

    if [ ! -f "${output_file}" ]; then
        log_error "Build completed but output file not found: ${output_file}"
        return 1
    fi

    log_success "Built ${os}/${arch} → bin/$(basename "${output_file}")"
}

# =============================================================================
# Post-build Functions
# =============================================================================

release_binary_names() {
    printf '%s\n' \
        "${APP_NAME}-linux-x86_64" \
        "${APP_NAME}-linux-arm64" \
        "${APP_NAME}-linux-armv7" \
        "${APP_NAME}-linux-x86" \
        "${APP_NAME}-windows-x86_64" \
        "${APP_NAME}-windows-x86" \
        "${APP_NAME}-darwin-arm64" \
        "${APP_NAME}-darwin-x86_64"
}

release_binary_is_expected() {
    local candidate="$1"
    local expected
    while IFS= read -r expected; do
        if [ "${candidate}" = "${expected}" ]; then
            return 0
        fi
    done < <(release_binary_names)
    return 1
}

clean_release_archives() {
    log_step "Cleaning old release archives"

    local archives_dir="${OUTPUT_DIR}/archives"
    local path
    if [ ! -d "${archives_dir}" ]; then
        log_error "Archives directory not found: ${archives_dir}"
        return 1
    fi
    for path in "${archives_dir}"/* "${archives_dir}"/.[!.]* "${archives_dir}"/..?*; do
        if [ -e "${path}" ] || [ -L "${path}" ]; then
            if ! rm -rf "${path}"; then
                log_error "Failed to remove old release artifact: ${path}"
                return 1
            fi
        fi
    done
    log_success "Cleaned old release archives"
}

create_archives() {
    log_step "Creating distribution archives"

    local archives_dir="${OUTPUT_DIR}/archives"
    local failed=0
    local basename_file
    local binary_name
    local extension
    local source_file
    local temporary_file
    local -a expected_binaries=()
    while IFS= read -r binary_name; do
        expected_binaries+=("${binary_name}")
    done < <(release_binary_names)

    if ! cp README.md LICENSE "${archives_dir}/" 2>/dev/null; then
        log_error "Failed to copy README.md and LICENSE to ${archives_dir}"
        return 1
    fi

    for basename_file in "${expected_binaries[@]}"; do
        source_file="${OUTPUT_DIR}/bin/${basename_file}"
        extension=""
        if [[ "${basename_file}" == *"-windows-"* ]]; then
            extension=".exe"
        fi
        temporary_file="${archives_dir}/${APP_NAME}${extension}"

        if [ ! -f "${source_file}" ]; then
            log_error "Binary not found: ${source_file}"
            failed=1
            continue
        fi
        if ! rm -f "${archives_dir}/${basename_file}.zip"; then
            log_error "Failed to remove old archive: ${archives_dir}/${basename_file}.zip"
            failed=1
            continue
        fi
        if ! cp "${source_file}" "${temporary_file}" 2>/dev/null; then
            log_error "Failed to copy ${source_file} to ${temporary_file}"
            failed=1
            continue
        fi

        if (cd "${archives_dir}" && zip -q "${basename_file}.zip" "${APP_NAME}${extension}" README.md LICENSE 2>/dev/null); then
            log_success "Archived: archives/${basename_file}.zip"
        else
            log_error "Failed to create archive: ${basename_file}.zip"
            failed=1
        fi
        if ! rm -f "${temporary_file}"; then
            log_error "Failed to remove temporary archive binary: ${temporary_file}"
            failed=1
        fi
        if [ ! -f "${archives_dir}/${basename_file}.zip" ]; then
            log_error "Archive was not created: ${archives_dir}/${basename_file}.zip"
            failed=1
        fi
    done

    if ! rm -f "${archives_dir}/README.md" "${archives_dir}/LICENSE"; then
        log_error "Failed to clean documentation files from ${archives_dir}"
        failed=1
    fi
    if [ "${failed}" -ne 0 ]; then
        return 1
    fi
    log_success "Created archives in ${archives_dir}/"
}

generate_checksums() {
    log_step "Generating checksums"

    local archives_dir="${OUTPUT_DIR}/archives"
    local binary_name
    local archive_name
    local checksum_count
    local -a expected_archives=()
    while IFS= read -r binary_name; do
        expected_archives+=("${binary_name}.zip")
    done < <(release_binary_names)

    if [ ! -d "${archives_dir}" ]; then
        log_error "Archives directory not found: ${archives_dir}"
        return 1
    fi
    for archive_name in "${expected_archives[@]}"; do
        if [ ! -f "${archives_dir}/${archive_name}" ]; then
            log_error "Expected archive not found: ${archives_dir}/${archive_name}"
            return 1
        fi
    done

    if command_exists md5sum; then
        if ! (cd "${archives_dir}" && md5sum "${expected_archives[@]}" >md5.txt.tmp 2>/dev/null); then
            log_error "Failed to generate archive checksums"
            rm -f "${archives_dir}/md5.txt.tmp"
            return 1
        fi
    elif command_exists md5; then
        if ! (cd "${archives_dir}" && md5 -r "${expected_archives[@]}" >md5.txt.tmp 2>/dev/null); then
            log_error "Failed to generate archive checksums"
            rm -f "${archives_dir}/md5.txt.tmp"
            return 1
        fi
    else
        log_error "No checksum command available (md5sum or md5)"
        return 1
    fi

    if ! mv "${archives_dir}/md5.txt.tmp" "${archives_dir}/md5.txt"; then
        log_error "Failed to finalize archive checksums"
        rm -f "${archives_dir}/md5.txt.tmp"
        return 1
    fi
    checksum_count=$(wc -l <"${archives_dir}/md5.txt")
    if [ "${checksum_count}" -ne "${#expected_archives[@]}" ]; then
        log_error "Expected ${#expected_archives[@]} archive checksum entries, found ${checksum_count}"
        return 1
    fi
    log_success "Generated checksums for ${checksum_count} release archives"
}

prepare_docker_binaries() {
    log_step "Preparing Docker binaries"

    local docker_dir="${OUTPUT_DIR}/docker"
    local failed=0
    local platform
    local arch
    local docker_platform
    local binary_name
    local source_file
    local target_file
    local -a platforms=(
        "x86_64:linux/amd64"
        "x86:linux/386"
        "armv7:linux/arm/v7"
        "arm64:linux/arm64"
    )

    if ! mkdir -p "${docker_dir}"; then
        log_error "Failed to create docker directory: ${docker_dir}"
        return 1
    fi

    for platform in "${platforms[@]}"; do
        arch="${platform%%:*}"
        docker_platform="${platform#*:}"
        binary_name="${APP_NAME}-linux-${arch}"
        source_file="${OUTPUT_DIR}/bin/${binary_name}"
        target_file="${docker_dir}/${docker_platform}/${APP_NAME}"

        if ! mkdir -p "${docker_dir}/${docker_platform}"; then
            log_error "Failed to create directory: ${docker_dir}/${docker_platform}"
            failed=1
            continue
        fi
        if [ ! -f "${source_file}" ]; then
            log_error "Binary not found: ${source_file}"
            failed=1
            continue
        fi
        if ! cp "${source_file}" "${target_file}" 2>/dev/null; then
            log_error "Failed to copy ${source_file} to ${target_file}"
            failed=1
            continue
        fi
        if [ ! -f "${target_file}" ]; then
            log_error "Docker binary was not created: ${target_file}"
            failed=1
            continue
        fi
        log_success "Copied bin/${binary_name} -> docker/${docker_platform}/${APP_NAME}"
    done

    if [ "${failed}" -ne 0 ]; then
        return 1
    fi
    log_success "Prepared Docker binaries in ${docker_dir}/"
}

validate_release_artifacts() {
    log_step "Validating release artifacts"

    local failed=0
    local binary_name
    local archive_file_name
    local docker_platform
    local checksum_count
    local archive_path
    local archive_name
    local path
    local entry_name
    local archive_count=0
    local archives_dir="${OUTPUT_DIR}/archives"
    local -a expected_binaries=()
    local -a expected_archives=()
    local -a docker_platforms=("linux/amd64" "linux/386" "linux/arm/v7" "linux/arm64")
    while IFS= read -r binary_name; do
        expected_binaries+=("${binary_name}")
        expected_archives+=("${binary_name}.zip")
    done < <(release_binary_names)

    for binary_name in "${expected_binaries[@]}"; do
        if [ ! -f "${OUTPUT_DIR}/bin/${binary_name}" ]; then
            log_error "Missing release binary: ${OUTPUT_DIR}/bin/${binary_name}"
            failed=1
        fi
        if [ ! -f "${archives_dir}/${binary_name}.zip" ]; then
            log_error "Missing release archive: ${archives_dir}/${binary_name}.zip"
            failed=1
        fi
    done
    for docker_platform in "${docker_platforms[@]}"; do
        if [ ! -f "${OUTPUT_DIR}/docker/${docker_platform}/${APP_NAME}" ]; then
            log_error "Missing Docker binary: ${OUTPUT_DIR}/docker/${docker_platform}/${APP_NAME}"
            failed=1
        fi
    done

    if [ ! -f "${archives_dir}/md5.txt" ]; then
        log_error "Missing release checksum asset: ${archives_dir}/md5.txt"
        failed=1
    else
        checksum_count=$(wc -l <"${archives_dir}/md5.txt")
        if [ "${checksum_count}" -ne "${#expected_archives[@]}" ]; then
            log_error "Expected ${#expected_archives[@]} archive checksum entries, found ${checksum_count}"
            failed=1
        fi
        if ! awk 'NF < 2 || length($1) != 32 || $1 !~ /^[0-9a-fA-F]+$/ { exit 1 }' "${archives_dir}/md5.txt"; then
            log_error "Release checksum asset contains an invalid MD5 entry"
            failed=1
        fi
        for archive_file_name in "${expected_archives[@]}"; do
            if ! awk -v expected="${archive_file_name}" '$NF == expected { found = 1 } END { exit found ? 0 : 1 }' "${archives_dir}/md5.txt"; then
                log_error "Missing checksum entry for ${archive_file_name}"
                failed=1
            fi
        done
    fi

    for archive_path in "${archives_dir}"/*.zip; do
        if [ ! -f "${archive_path}" ]; then
            continue
        fi
        archive_count=$((archive_count + 1))
        archive_name="${archive_path##*/}"
        archive_name="${archive_name%.zip}"
        if ! release_binary_is_expected "${archive_name}"; then
            log_error "Unexpected release archive: ${archive_path}"
            failed=1
        fi
    done
    if [ "${archive_count}" -ne "${#expected_binaries[@]}" ]; then
        log_error "Expected ${#expected_binaries[@]} release archives, found ${archive_count}"
        failed=1
    fi

    for path in "${archives_dir}"/* "${archives_dir}"/.[!.]* "${archives_dir}"/..?*; do
        if [ -e "${path}" ] || [ -L "${path}" ]; then
            entry_name="${path##*/}"
            case "${entry_name}" in
            md5.txt)
                ;;
            *.zip)
                if [ ! -f "${path}" ]; then
                    log_error "Release archive is not a regular file: ${path}"
                    failed=1
                fi
                ;;
            *)
                log_error "Unexpected release archive entry: ${path}"
                failed=1
                ;;
            esac
        fi
    done

    if [ "${failed}" -ne 0 ]; then
        return 1
    fi
    log_success "Release artifact manifest is complete"
}

# =============================================================================
# Main Execution
# =============================================================================

show_usage() {
    echo "Usage: $0 <command> [os] [arch]"
    echo ""
    echo "Commands:"
    echo "  release              Build all platforms and create distribution packages"
    echo "  build <os> <arch>    Build for specific OS and architecture"
    echo "  help                 Show this help message"
    echo ""
    echo "Supported OS:"
    echo "  linux, windows, darwin, android"
    echo ""
    echo "Supported architectures:"
    echo "  x86_64, arm64, armv7, x86"
    echo ""
    echo "Examples:"
    echo "  $0 build windows x86_64"
    echo "  $0 build linux x86_64"
    echo "  $0 build android arm64"
    echo "  $0 release"
}

validate_os_arch() {
    local os="$1"
    local arch="$2"

    # Validate OS
    case "$os" in
    "linux" | "windows" | "darwin" | "android") ;;
    *)
        log_error "Unsupported OS: $os"
        log_error "Supported OS: linux, windows, darwin, android"
        return 1
        ;;
    esac

    # Validate architecture
    case "$arch" in
    "x86_64" | "arm64" | "armv7" | "x86") ;;
    *)
        log_error "Unsupported architecture: $arch"
        log_error "Supported architectures: x86_64, arm64, armv7, x86"
        return 1
        ;;
    esac

    return 0
}

main() {
    case "${1:-}" in
    "build")
        if [ $# -ne 3 ]; then
            log_error "Build command requires OS and architecture"
            log_error "Usage: $0 build <os> <arch>"
            show_usage
            exit 1
        fi

        local os="$2"
        local arch="$3"

        if ! validate_os_arch "$os" "$arch"; then
            exit 1
        fi

        log_step "Starting single platform build"
        echo "📦 Building ${APP_NAME} ${GIT_VERSION} (${COMMIT_ID}) for ${os}/${arch}"
        echo ""

        # Setup
        if ! prepare_environment; then
            log_error "Failed to prepare build environment"
            exit 1
        fi

        # Build frontend
        if ! build_frontend; then
            log_error "Failed to build frontend"
            exit 1
        fi

        # Update price
        if ! update_price; then
            log_error "Failed to update price"
            exit 1
        fi

        # Build for specified platform
        log_step "Building binary"

        if ! build_standard "$os" "$arch"; then
            log_error "Failed to build ${os}/${arch}"
            exit 1
        fi

        log_step "Build completed"
        log_success "Binary ready: ${OUTPUT_DIR}/bin/${APP_NAME}-${os}-${arch}"
        ;;
    "release")
        log_step "Starting release build"
        echo "📦 Building ${APP_NAME} ${GIT_VERSION} (${COMMIT_ID})"
        echo ""

        # Setup
        if ! prepare_environment; then
            log_error "Failed to prepare build environment"
            exit 1
        fi
        if ! clean_release_archives; then
            log_error "Failed to clean old release archives"
            exit 1
        fi

        # Build frontend
        if ! build_frontend; then
            log_error "Failed to build frontend"
            exit 1
        fi

        # Update price
        if ! update_price; then
            log_error "Failed to update price"
            exit 1
        fi

        # Build for different platforms
        log_step "Building binaries"
        local release_failed=0

        # Standard builds (pure Go, static binaries)
        if ! build_standard linux x86_64; then
            log_error "Failed to build Linux x86_64"
            release_failed=1
        fi
        if ! build_standard linux arm64; then
            log_error "Failed to build Linux arm64"
            release_failed=1
        fi
        if ! build_standard linux armv7; then
            log_error "Failed to build Linux armv7"
            release_failed=1
        fi
        if ! build_standard linux x86; then
            log_error "Failed to build Linux x86"
            release_failed=1
        fi
        if ! build_standard windows x86_64; then
            log_error "Failed to build Windows x86_64"
            release_failed=1
        fi
        if ! build_standard windows x86; then
            log_error "Failed to build Windows x86"
            release_failed=1
        fi
        if ! build_standard darwin arm64; then
            log_error "Failed to build Darwin arm64"
            release_failed=1
        fi
        if ! build_standard darwin x86_64; then
            log_error "Failed to build Darwin x86_64"
            release_failed=1
        fi

        if [ "${release_failed}" -ne 0 ]; then
            log_error "One or more release platform builds failed"
            exit 1
        fi

        # Post-processing
        if ! prepare_docker_binaries; then
            log_error "Failed to prepare Docker binaries"
            exit 1
        fi
        if ! create_archives; then
            log_error "Failed to create archives"
            exit 1
        fi
        if ! generate_checksums; then
            log_error "Failed to generate checksums"
            exit 1
        fi
        if ! validate_release_artifacts; then
            log_error "Release artifact validation failed"
            exit 1
        fi

        log_step "Build completed"
        log_success "All artifacts ready in ${OUTPUT_DIR}/"
        log_info "  • Binaries: ${OUTPUT_DIR}/bin/"
        log_info "  • Docker binaries: ${OUTPUT_DIR}/docker/"
        log_info "  • Archives: ${OUTPUT_DIR}/archives/"
        ;;
    "help" | "-h" | "--help")
        show_usage
        ;;
    "")
        log_error "No command specified"
        show_usage
        exit 1
        ;;
    *)
        log_error "Unknown command: $1"
        show_usage
        exit 1
        ;;
    esac
}

main "$@"
