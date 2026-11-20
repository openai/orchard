//go:build unix

package privdrop

import (
	"errors"
	"fmt"
	"os"
	userpkg "os/user"
	"strconv"

	"golang.org/x/sys/unix"
)

var errFailed = errors.New("failed to drop privileges")

func Drop(username string) error {
	user, err := userpkg.Lookup(username)
	if err != nil {
		return fmt.Errorf("%w: failed to lookup username %q: %v",
			errFailed, username, err)
	}

	gid, err := strconv.Atoi(user.Gid)
	if err != nil {
		return fmt.Errorf("%w: failed to parse %s's group ID %q: %v",
			errFailed, user.Name, user.Gid, err)
	}

	if err := unix.Setregid(gid, gid); err != nil {
		return fmt.Errorf("%w: failed to set real and effective group ID to %d: %v",
			errFailed, gid, err)
	}

	if err := os.Setenv("HOME", user.HomeDir); err != nil {
		return fmt.Errorf("%w: failed to set new HOME to %q: %v",
			errFailed, user.HomeDir, err)
	}

	uid, err := strconv.Atoi(user.Uid)
	if err != nil {
		return fmt.Errorf("%w: failed to parse %s's user ID %q: %v",
			errFailed, user.Name, user.Uid, err)
	}

	if err := unix.Setreuid(uid, uid); err != nil {
		return fmt.Errorf("%w: failed to set real and effective user ID to %d: %v",
			errFailed, uid, err)
	}

	return nil
}
