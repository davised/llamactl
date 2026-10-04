package manager

// Manager-side support for instances whose server runs elsewhere (external
// services and Slurm jobs; see pkg/instance/driver.go). Kept apart from
// operations.go so the upstream code only carries one-line hooks.

import (
	"fmt"
	"llamactl/pkg/instance"
	"llamactl/pkg/validation"
)

// validateDriver checks an instance's external or Slurm options.
func (im *instanceManager) validateDriver(options *instance.Options) error {
	if options.External != nil && options.Slurm != nil {
		return validation.ValidationError(fmt.Errorf("an instance cannot be both external and a Slurm job"))
	}
	if options.Slurm != nil {
		if err := options.Slurm.Validate(); err != nil {
			return validation.ValidationError(err)
		}
		// The host is the node the job lands on; only the port is fixed
		if options.BackendOptions.GetPort() <= 0 {
			return validation.ValidationError(fmt.Errorf("slurm instances require a port in backend options"))
		}
	}
	if options.External != nil {
		if err := options.External.Validate(); err != nil {
			return validation.ValidationError(err)
		}
		if options.BackendOptions.GetHost() == "" {
			return validation.ValidationError(fmt.Errorf("external instances require a host in backend options"))
		}
		if options.BackendOptions.GetPort() <= 0 {
			return validation.ValidationError(fmt.Errorf("external instances require a port in backend options"))
		}
	}
	return nil
}

// validateDriverUpdate validates new options for an existing instance. The
// driver is chosen at creation and can't change.
func (im *instanceManager) validateDriverUpdate(name string, options *instance.Options) error {
	if err := im.validateDriver(options); err != nil {
		return err
	}
	if inst, ok := im.registry.get(name); ok && driverKind(options) != driverKind(inst.GetOptions()) {
		return validation.ValidationError(fmt.Errorf("cannot switch instance %s between local, external and Slurm execution; delete and recreate it", name))
	}
	return nil
}

// driverKind names how an instance's server runs: "local", "external" or "slurm".
func driverKind(options *instance.Options) string {
	switch {
	case options == nil:
		return "local"
	case options.Slurm != nil:
		return "slurm"
	case options.External != nil:
		return "external"
	}
	return "local"
}
