package instance

// Statuses used only by drivers (see driver.go). They are registered here
// rather than in status.go so the upstream enum stays untouched; their
// values start well above the upstream ones so the two can't collide.
const (
	// Pending: a Slurm job is queued, waiting for an allocation
	Pending Status = 100 + iota
	// Starting: a Slurm job is running but its server isn't healthy yet
	Starting
)

func init() {
	for s, name := range map[Status]string{Pending: "pending", Starting: "starting"} {
		statusToName[s] = name
		nameToStatus[name] = s
	}
}

// IsActive reports whether the status is running or on its way to running
// (a queued or starting Slurm job).
func (s Status) IsActive() bool {
	return s == Running || s == Pending || s == Starting
}

// PersistedStatusName maps a status name to one the database accepts. The
// instances table's CHECK constraint allows only the upstream statuses, and
// widening it means rebuilding the table, which would cascade-delete
// key_permissions. The driver-only states are transitional, so they are
// stored as "restarting"; Slurm instances re-read their real state from
// squeue on startup.
func PersistedStatusName(name string) string {
	switch name {
	case "pending", "starting":
		return "restarting"
	}
	return name
}
