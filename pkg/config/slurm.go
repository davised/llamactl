package config

import "time"

// SlurmConfig contains settings for instances that run as Slurm jobs. Zero
// values mean the defaults below, so an absent section needs no handling.
type SlurmConfig struct {
	// How often squeue is polled to update Slurm instances (default 15s)
	PollInterval time.Duration `yaml:"poll_interval,omitempty" json:"poll_interval,omitempty" swaggertype:"string" example:"15s"`

	// Prepended to every Slurm command, e.g. ["ssh", "-o", "BatchMode=yes", "submit01"]
	// when llamactl doesn't run on a host that can submit jobs
	CommandPrefix []string `yaml:"command_prefix,omitempty" json:"command_prefix,omitempty"`

	// Turns a compute node name from squeue into the host llamactl connects
	// to; "{node}" is replaced with the node name (default "{node}"), e.g.
	// "{node}.cluster.example.edu"
	NodeHostTemplate string `yaml:"node_host_template,omitempty" json:"node_host_template,omitempty"`
}
