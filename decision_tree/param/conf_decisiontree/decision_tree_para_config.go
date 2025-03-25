package conf_decisiontree

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/call"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/cmd"
	tree_conf "gitlab.grandhoo.com/rock/rock_v3/decision_tree/conf/tree"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/ml/tree"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/param/conf_manager"
)

var CMD_DECISIONTREE = true

func Init() {
	var decisionTreeSettings = cmd.NewSecondaryValue(make(map[string]cmd.Value), nil)
	// 添加一些命令行参数
	err := conf_manager.AddCmdArgs(
		&cmd.Flag{
			Name:      "decision-tree",
			Aliases:   []string{"DTree"},
			Usage:     "--decision-tree --DTree, specify the decisionTreeMining parameters",
			Required:  false,
			FlagValue: decisionTreeSettings,
		},
	)
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("min-support", cmd.NewFloat64Value(&tree_conf.MinSupportRate, func(valueToCheck float64) error {
		if !(valueToCheck > 0 && valueToCheck <= 1) {
			return fmt.Errorf("expected value in range (0,1], but got '%v'", valueToCheck)
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("switch", &cmd.BoolCmdValue{Destination: &CMD_DECISIONTREE})
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("filter", &cmd.BoolCmdValue{Destination: &tree_conf.FilterFlag})
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("y-flag", &cmd.BoolCmdValue{Destination: &tree_conf.YFlag})
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("corr-flag", &cmd.BoolCmdValue{Destination: &tree_conf.CorrFlag})
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("tree-num", cmd.NewIntValue(&tree_conf.TreeNum, func(valueToCheck int) error {
		if valueToCheck < 1 {
			return fmt.Errorf("expected value in range [1, ∞), but got '%v'", valueToCheck)
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("select-num", cmd.NewIntValue(&tree_conf.SelectNum, func(valueToCheck int) error {
		if valueToCheck < 1 {
			return fmt.Errorf("expected value in range [1, ∞), but got '%v'", valueToCheck)
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("top-col", cmd.NewIntValue(&tree_conf.TopCol, func(valueToCheck int) error {
		if valueToCheck < 1 {
			return fmt.Errorf("expected value in range [1, ∞), but got '%v'", valueToCheck)
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("one-hot-strategy", cmd.NewIntValue(&tree_conf.OneHotStrategy, func(valueToCheck int) error {
		if valueToCheck < 1 {
			return fmt.Errorf("expected value in range [1, ∞), but got '%v'", valueToCheck)
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("max-instance", cmd.NewIntValue(&tree_conf.MaxInstance, func(valueToCheck int) error {
		if valueToCheck < 1 {
			return fmt.Errorf("expected value in range [1, ∞), but got '%v'", valueToCheck)
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("max-depth", cmd.NewIntValue(&tree_conf.MaxTreeDepth, func(valueToCheck int) error {
		if valueToCheck < 1 {
			return fmt.Errorf("expected value in range [1, ∞), but got '%v'", valueToCheck)
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("max-leaf-num", cmd.NewIntValue(&tree_conf.MaxLeafNum, func(valueToCheck int) error {
		if valueToCheck < 1 {
			return fmt.Errorf("expected value in range [1, ∞), but got '%v'", valueToCheck)
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("co-worker-num", cmd.NewIntValue(&tree.CoWorkerNum, func(valueToCheck int) error {
		if valueToCheck < 0 {
			return fmt.Errorf("expected value in range [0, ∞), but got '%v'", valueToCheck)
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("max-feature-num", cmd.NewUint32Value(&tree_conf.MaxFeatureNum, func(valueToCheck uint32) error {
		if valueToCheck < 1 {
			return fmt.Errorf("at least one feature! ")
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("criterion", cmd.NewStringValue(&tree_conf.ImpurityCriterion, func(valueToCheck string) error {
		switch valueToCheck {
		case tree.Entropy{}.String():
			return nil
		case tree.Gini{}.String():
			return nil
		default:
			return fmt.Errorf("unknown criterion:%s", valueToCheck)
		}
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("instance-keep-num", cmd.NewIntValue(&tree.InstanceKeepNum, func(valueToCheck int) error {
		if valueToCheck < 0 {
			return fmt.Errorf("expected value in range [0, ∞), but got '%v'", valueToCheck)
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("min-impurity-decrease", cmd.NewFloat64Value(&tree_conf.MinImpurityDecrease, func(valueToCheck float64) error {
		if valueToCheck < 0 {
			return fmt.Errorf("need a non-negtivate value")
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("weight-down", &cmd.BoolCmdValue{Destination: &tree_conf.WeightDownByPivot})
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("min-confidence", cmd.NewFloat64Value(&tree_conf.MinConfidence, func(valueToCheck float64) error {
		if valueToCheck < 0 || valueToCheck > 1 {
			return fmt.Errorf("expected value in range [0,1], but got '%v'", valueToCheck)
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	err = decisionTreeSettings.AddSubValue("value-interval-num", cmd.NewIntValue(&call.IntervalNum, func(valueToCheck int) error {
		if valueToCheck < 1 {
			return fmt.Errorf("interval-num should not be less than 1, got %d", valueToCheck)
		}
		return nil
	}))
	err = decisionTreeSettings.AddSubValue("value-limited-numeric-threshold", cmd.NewIntValue(&call.LimitedNumericValueNumLimit, func(valueToCheck int) error {
		if valueToCheck < 1 {
			return fmt.Errorf("limited-numeric should have limit larger than 1, got %d", valueToCheck)
		}
		return nil
	}))
}
