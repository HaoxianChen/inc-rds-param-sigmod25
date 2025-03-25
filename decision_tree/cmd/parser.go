package cmd

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Flag 基础结构，命令行中一项参数
type Flag struct {
	Name     string   // 参数名
	Aliases  []string // 别名
	Usage    string   // 使用帮助
	Required bool     // 是否是必须设置的

	FlagValue Value // Flag中存储的具体值
}

func (flag Flag) String() string {
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("--%s:", flag.Name))
	builder.WriteString(flag.FlagValue.String())
	return builder.String()
}

// fit 看是否与当前flag同名，或者是它的一个别名
func (flag *Flag) fit(oneName string) bool {
	if (*flag).Name == oneName {
		return true
	}
	for _, name := range (*flag).Aliases {
		if name == oneName {
			return true
		}
	}
	return false
}

type FlagContainer struct {
	flags              []*Flag
	flagsPrintPriority map[string]int
}

func (container *FlagContainer) GetFlags() []*Flag {
	return container.flags
}

func (container *FlagContainer) SetPrintPriority(flagsPrintPriority map[string]int) {
	container.flagsPrintPriority = flagsPrintPriority
}

func (container *FlagContainer) GetPrintPriority() map[string]int {
	return container.flagsPrintPriority
}

func (container FlagContainer) String() string {
	// 可以按名字排个序
	sortedFlags := make([]*Flag, len(container.flags))
	copy(sortedFlags, container.flags)
	sort.Slice(sortedFlags, func(i, j int) bool {
		return sortedFlags[i].Name < sortedFlags[j].Name
	})

	builder := strings.Builder{}
	for _, flag := range sortedFlags {
		builder.WriteString((*flag).String())
		builder.WriteString("\n")
	}

	return builder.String()
}

func NewFlagContainer() *FlagContainer {
	return &FlagContainer{}
}

// AddFlags 添加一些新的flag
func (container *FlagContainer) AddFlags(flags ...*Flag) error {
	for _, flagToBeAdded := range flags {
		// 先判断一下是否重名
		// 如果有别的一些命名限制，也可以加在这里
		for _, flagToCheck := range (*container).flags {
			if flagToCheck.fit((*flagToBeAdded).Name) {
				return fmt.Errorf("already existed flag with name or alias:%s", (*flagToBeAdded).Name)
			}
			for _, alias := range (*flagToBeAdded).Aliases {
				if flagToCheck.fit(alias) {
					return fmt.Errorf("already existed flag with name or alias:%s", alias)
				}
			}
		}
		// 加入
		(*container).flags = append((*container).flags, flagToBeAdded)
	}
	return nil
}

// Parse 解析输入的命令行参数
func (container *FlagContainer) Parse(args []string) error {
	// 遍历输入的args，去找匹配的flag
	curFlags := (*container).flags // 当前要处理哪些
	if len(curFlags) == 0 {
		if len(args) != 0 {
			return errors.New("too many args for container to parse")
		}
		return nil
	}

	argPattern := regexp.MustCompile(`^-{2}([^-].*)`) // fixme: 获取参数的名字，也就是--p中的p，不支持缩写-p，因为还有负数什么的，以后可以再考虑一下，就是-后面以a-z或A-Z这种开始当作是命令
	argIndex := 0                                     // 遍历输入参数
	flagsBeParsed := make([]*Flag, 0, len(curFlags))  // 最后是期望全部被解析
	for argIndex < len(args) && len(curFlags) > 0 {
		argName := argPattern.FindStringSubmatch(args[argIndex]) // [完整匹配，捕获的匹配]
		if len(argName) != 2 {
			// 不是--p
			return fmt.Errorf("arg doesn't start with '--<arg name>':'%s'", args[argIndex])
		} else {
			matchedFlagIndex := -1
			for flagIndex, flag := range curFlags {
				if flag.fit(argName[1]) {
					matchedFlagIndex = flagIndex // 是该flag对应的
					argIndex += 1
					argValue := []string(nil)
					for argIndex < len(args) && len(argPattern.FindStringSubmatch(args[argIndex])) == 0 {
						// 是这个arg下带的子参数
						argValue = append(argValue, args[argIndex])
						argIndex += 1
					}
					err := flag.FlagValue.Set(argValue)
					if err != nil {
						return fmt.Errorf("error in set value for arg:'%s', <%s>", argName[1], err.Error())
					}
					break
				}
			}
			if matchedFlagIndex == -1 {
				// 当前arg没有命中任何flag，冗余的参数？
				// 这里还有一种可能，就是一个参数重复写了两遍，那第一次被解析之后那个flag就被消耗了，第二次就无法解析了
				for _, flag := range flagsBeParsed {
					// fixme:这里可以改，如果允许重复参数的话，比如只取第一次遇到的，或最后一次遇到的这样
					if flag.fit(argName[1]) {
						return fmt.Errorf("duplicated arg:'%s'", argName[1])
					}
				}
				return fmt.Errorf("unexpected arg:'%s'", argName[1])
			} else {
				// 有flag被消耗了
				flagsBeParsed = append(flagsBeParsed, curFlags[matchedFlagIndex])
				// 从剩余flags中移除
				curFlags[matchedFlagIndex], curFlags[0] = curFlags[0], curFlags[matchedFlagIndex]
				curFlags = curFlags[1:]
			}
		}
	}
	// 如果len(curFlags) > 0就时还有一些flag本次没有设置
	err := error(nil)
	if len(curFlags) > 0 {
		for _, flag := range curFlags {
			if flag.Required {
				// 有一项必须设置的没有设置
				if err == nil {
					err = fmt.Errorf("flag is required but not set: '%s'", (*flag).Usage)
				} else {
					err = fmt.Errorf("%s\nflag is required but not set: '%s'", err.Error(), (*flag).Usage)
				}
			}
		}
	}
	// fixme:这里可以再考虑一下，要不要一次把命令行都检查完，把所有的错误都返回，
	// fixme:这样感觉挺好，但是可能因为一个错误使得后面很多都出错，就没什么必要把后面的都打出来
	return err
}

type Value interface {
	Set(rawValue []string) error // Set 将读入的string参数值转换为需要的类型，这里用一个[]string，既是为了二级参数，也可以给一级参数做检查，多设置了或者忘了设置
	Get() map[string]string      //Get 将Value值取出来，之前的打印如果换种打印方式拿不到具体的值,这里也是为了打印
	String() string              // 为了打印
	// Check() error // Check 检查当前value是否合法，如需要输入非0等，这个放到Set里直接做吧
}

// NoArgBoolValue 不带额外参数的value，只要命令行存在这个value就destination置true
type NoArgBoolValue struct {
	destination *bool
}

func NewNoArgBoolValue(destination *bool) *NoArgBoolValue {
	return &NoArgBoolValue{
		destination: destination,
	}
}

func (value *NoArgBoolValue) Set(rawValue []string) error {
	if len(rawValue) > 0 {
		return errors.New("too many values for this arg")
	}

	*(*value).destination = true
	return nil
}

func (value *NoArgBoolValue) String() string {
	if *(*value).destination {
		return "true"
	} else {
		return "false"
	}
}

func (value *NoArgBoolValue) Get() map[string]string {
	return map[string]string{"firstParaValue": value.String()}
}

type StringValue struct {
	destination *string
	validate    func(valueToCheck string) error
}

func NewStringValue(destination *string, validateFunc func(valueToCheck string) error) *StringValue {
	return &StringValue{
		destination: destination,
		validate:    validateFunc,
	}
}

func (value *StringValue) Set(rawValue []string) error {
	if value == nil || (*value).destination == nil {
		return errors.New("no destination to store values")
	}
	if len(rawValue) == 0 {
		return errors.New("too few values for this arg，forget to set? ")
	}
	if len(rawValue) > 1 {
		return errors.New("too many values for this arg")
	}

	if (*value).validate != nil {
		if err := (*value).validate(rawValue[0]); err != nil {
			return fmt.Errorf("validate falied!===>%s", err.Error())
		}
	}
	*(*value).destination = rawValue[0]
	return nil
}

func (value *StringValue) String() string {
	return *(*value).destination
}

func (value *StringValue) Get() map[string]string {
	return map[string]string{"firstParaValue": *(*value).destination}
}

type IntValue struct {
	destination *int
	validate    func(valueToCheck int) error
}

func NewIntValue(destination *int, validateFunc func(valueToCheck int) error) *IntValue {
	return &IntValue{
		destination: destination,
		validate:    validateFunc,
	}
}

func (value *IntValue) Set(rawValue []string) error {
	if value == nil || (*value).destination == nil {
		return errors.New("no destination to store values")
	}
	if len(rawValue) == 0 {
		return errors.New("too few values for this arg，forget to set? ")
	}
	if len(rawValue) > 1 {
		return errors.New("too many values for this arg")
	}

	intValue, err := strconv.Atoi(rawValue[0])
	if err != nil {
		return fmt.Errorf("can't use '%s' as integer values", rawValue[0])
	}

	if (*value).validate != nil {
		if err = (*value).validate(intValue); err != nil {
			return fmt.Errorf("validate falied!===>%s", err.Error())
		}
	}

	*(*value).destination = intValue
	return nil
}

func (value *IntValue) String() string {
	return fmt.Sprintf("%d", *(*value).destination)
}

func (value *IntValue) Get() map[string]string {
	return map[string]string{"firstParaValue": fmt.Sprintf("%d", *(*value).destination)}
}

type Uint64Value struct {
	destination *uint64
	validate    func(valueToCheck uint64) error
}

func NewUint64Value(destination *uint64, validateFunc func(valueToCheck uint64) error) *Uint64Value {
	return &Uint64Value{
		destination: destination,
		validate:    validateFunc,
	}
}

func (value *Uint64Value) Set(rawValue []string) error {
	if value == nil || (*value).destination == nil {
		return errors.New("no destination to store values")
	}
	if len(rawValue) == 0 {
		return errors.New("too few values for this arg，forget to set? ")
	}
	if len(rawValue) > 1 {
		return errors.New("too many values for this arg")
	}

	uint64Value, err := strconv.ParseUint(rawValue[0], 10, 64)
	if err != nil {
		return errors.New("can't use '" + rawValue[0] + "' as uint64 values")
	}

	if (*value).validate != nil {
		if err = (*value).validate(uint64Value); err != nil {
			return fmt.Errorf("validate falied!===>%s", err.Error())
		}
	}

	*(*value).destination = uint64Value
	return nil
}

func (value *Uint64Value) String() string {
	return fmt.Sprintf("%d", *(*value).destination)
}

func (value *Uint64Value) Get() map[string]string {
	return map[string]string{"firstParaValue": fmt.Sprintf("%d", *(*value).destination)}
}

type Uint32Value struct {
	destination *uint32
	validate    func(valueToCheck uint32) error
}

func NewUint32Value(destination *uint32, validateFunc func(valueToCheck uint32) error) *Uint32Value {
	return &Uint32Value{
		destination: destination,
		validate:    validateFunc,
	}
}

func (value *Uint32Value) Set(rawValue []string) error {
	if value == nil || (*value).destination == nil {
		return errors.New("no destination to store values")
	}
	if len(rawValue) == 0 {
		return errors.New("too few values for this arg，forget to set? ")
	}
	if len(rawValue) > 1 {
		return errors.New("too many values for this arg")
	}

	uint32Value, err := strconv.ParseUint(rawValue[0], 10, 32)
	if err != nil {
		return errors.New("can't use '" + rawValue[0] + "' as uint32 values")
	}

	if (*value).validate != nil {
		if err = (*value).validate(uint32(uint32Value)); err != nil {
			return fmt.Errorf("validate falied!===>%s", err.Error())
		}
	}

	*(*value).destination = uint32(uint32Value)
	return nil
}

func (value *Uint32Value) String() string {
	return fmt.Sprintf("%d", *(*value).destination)
}

func (value *Uint32Value) Get() map[string]string {
	return map[string]string{"firstParaValue": fmt.Sprintf("%d", *(*value).destination)}
}

type Float64Value struct {
	destination *float64
	validate    func(valueToCheck float64) error
}

func NewFloat64Value(destination *float64, validateFunc func(valueToCheck float64) error) *Float64Value {
	return &Float64Value{
		destination: destination,
		validate:    validateFunc,
	}
}

func (value *Float64Value) Set(rawValue []string) error {
	if value == nil || (*value).destination == nil {
		return errors.New("no destination to store values")
	}
	if len(rawValue) == 0 {
		return errors.New("too few values for this arg，forget to set? ")
	}
	if len(rawValue) > 1 {
		return errors.New("too many values for this arg")
	}

	float64Value, err := strconv.ParseFloat(rawValue[0], 64)
	if err != nil {
		err = errors.New("can't use '" + rawValue[0] + "' as float64 value")
	}

	if (*value).validate != nil {
		if err = (*value).validate(float64Value); err != nil {
			return fmt.Errorf("validate falied!===>%s", err.Error())
		}
	}

	*(*value).destination = float64Value
	return nil
}

func (value *Float64Value) String() string {
	return fmt.Sprintf("%g", *(*value).destination)
}

func (value *Float64Value) Get() map[string]string {
	return map[string]string{"firstParaValue": fmt.Sprintf("%g", *(*value).destination)}
}

type Float32Value struct {
	destination *float32
	validate    func(valueToCheck float32) error
}

func NewFloat32Value(destination *float32, validateFunc func(valueToCheck float32) error) *Float32Value {
	return &Float32Value{
		destination: destination,
		validate:    validateFunc,
	}
}

func (value *Float32Value) Set(rawValue []string) error {
	if value == nil || (*value).destination == nil {
		return errors.New("no destination to store values")
	}
	if len(rawValue) == 0 {
		return errors.New("too few values for this arg，forget to set? ")
	}
	if len(rawValue) > 1 {
		return errors.New("too many values for this arg")
	}

	float32Value, err := strconv.ParseFloat(rawValue[0], 32)
	if err != nil {
		err = errors.New("can't use '" + rawValue[0] + "' as float32 value")
	}

	if (*value).validate != nil {
		if err = (*value).validate(float32(float32Value)); err != nil {
			return fmt.Errorf("validate falied!===>%s", err.Error())
		}
	}

	*(*value).destination = float32(float32Value)
	return nil
}

func (value *Float32Value) String() string {
	return fmt.Sprintf("%g", *(*value).destination)
}

func (value *Float32Value) Get() map[string]string {
	return map[string]string{"firstParaValue": fmt.Sprintf("%g", *(*value).destination)}
}

type IntListValue struct {
	destination *[]int
	validate    func(valueToCheck []int) error
}

func NewIntListValue(destination *[]int, validateFunc func(valueToCheck []int) error) *IntListValue {
	return &IntListValue{
		destination: destination,
		validate:    validateFunc,
	}
}

func (value *IntListValue) Set(rawValue []string) (err error) {
	if value == nil || (*value).destination == nil {
		err = errors.New("no destination to store values")
		return
	}
	if len(rawValue) == 0 {
		err = errors.New("too few values for this arg，forget to set? ")
		return
	}

	num := len(rawValue)
	intListValue := make([]int, num)
	for i := 0; i < num; i++ {
		intListValue[i], err = strconv.Atoi(rawValue[i])
		if err != nil {
			err = fmt.Errorf("can't use '%s' as integer values", rawValue[i])
			return
		}
	}

	if (*value).validate != nil {
		if err = (*value).validate(intListValue); err != nil {
			err = fmt.Errorf("validate falied!===>%s", err.Error())
			return
		}
	}

	*(*value).destination = intListValue
	return
}

func (value *IntListValue) String() string {
	return fmt.Sprintf("%v", *(*value).destination)
}

func (value *IntListValue) Get() map[string]string {
	return map[string]string{"firstParaValue": fmt.Sprintf("%v", *(*value).destination)}
}

type Uint64ListValue struct {
	destination *[]uint64
	validate    func(valueToCheck []uint64) error
}

func NewUint64ListValue(destination *[]uint64, validateFunc func(valueToCheck []uint64) error) *Uint64ListValue {
	return &Uint64ListValue{
		destination: destination,
		validate:    validateFunc,
	}
}

func (value *Uint64ListValue) Set(rawValue []string) (err error) {
	if value == nil || (*value).destination == nil {
		err = errors.New("no destination to store values")
		return
	}
	if len(rawValue) == 0 {
		err = errors.New("too few values for this arg，forget to set? ")
		return
	}

	num := len(rawValue)
	uint64ListValue := make([]uint64, num)
	for i := 0; i < num; i++ {
		uint64ListValue[i], err = strconv.ParseUint(rawValue[i], 10, 64)
		if err != nil {
			err = fmt.Errorf("can't use '%s' as uint64 values", rawValue[i])
			return
		}
	}

	if (*value).validate != nil {
		if err = (*value).validate(uint64ListValue); err != nil {
			err = fmt.Errorf("validate falied!===>%s", err.Error())
			return
		}
	}

	*(*value).destination = uint64ListValue
	return
}

func (value *Uint64ListValue) String() string {
	return fmt.Sprintf("%v", *(*value).destination)
}

func (value *Uint64ListValue) Get() map[string]string {
	return map[string]string{"firstParaValue": fmt.Sprintf("%v", *(*value).destination)}
}

type Uint32ListValue struct {
	destination *[]uint32
	validate    func(valueToCheck []uint32) error
}

func NewUint32ListValue(destination *[]uint32, validateFunc func(valueToCheck []uint32) error) *Uint32ListValue {
	return &Uint32ListValue{
		destination: destination,
		validate:    validateFunc,
	}
}

func (value *Uint32ListValue) Set(rawValue []string) error {
	if value == nil || (*value).destination == nil {
		return errors.New("no destination to store values")
	}
	if len(rawValue) == 0 {
		return errors.New("too few values for this arg，forget to set? ")
	}

	num := len(rawValue)
	uint32ListValue := make([]uint32, num)
	for i := 0; i < num; i++ {
		asUint64, err := strconv.ParseUint(rawValue[i], 10, 32)
		if err != nil {
			return fmt.Errorf("can't use '%s' as uint32 values", rawValue[i])
		}
		uint32ListValue[i] = uint32(asUint64)
	}

	if (*value).validate != nil {
		if err := (*value).validate(uint32ListValue); err != nil {
			return fmt.Errorf("validate falied!===>%s", err.Error())
		}
	}

	*(*value).destination = uint32ListValue
	return nil
}

func (value *Uint32ListValue) String() string {
	return fmt.Sprintf("%v", *(*value).destination)
}

func (value *Uint32ListValue) Get() map[string]string {
	return map[string]string{"firstParaValue": fmt.Sprintf("%v", *(*value).destination)}
}

type AnyListValue struct {
	values   []Value
	validate func(valueToCheck []Value) error
}

func NewAnyListValue(values []Value, validateFunc func(valueToCheck []Value) error) *AnyListValue {
	return &AnyListValue{
		values:   values,
		validate: validateFunc,
	}
}

func (value *AnyListValue) Set(rawValue []string) error {
	if value == nil || len((*value).values) == 0 {
		return errors.New("no destination to store values")
	}
	if len(rawValue) == 0 {
		return errors.New("too few values for this arg，forget to set? ")
	}

	if len((*value).values) != len(rawValue) {
		return errors.New("values num not consistent")
	}

	num := len(rawValue)
	for i := 0; i < num; i++ {
		if err := (*value).values[i].Set([]string{rawValue[i]}); err != nil {
			return fmt.Errorf("in AnyList:%s", err.Error())
		}
	}

	if (*value).validate != nil {
		if err := (*value).validate((*value).values); err != nil {
			return fmt.Errorf("validate falied!===>%s", err.Error())
		}
	}

	return nil
}

func (value *AnyListValue) String() string {
	builder := strings.Builder{}
	builder.WriteString("[")
	for i, oneValue := range (*value).values {
		if i != 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(oneValue.String())
	}
	builder.WriteString("]")

	return builder.String()
}

func (value *AnyListValue) Get() map[string]string {
	builder := strings.Builder{}
	builder.WriteString("[")
	for i, oneValue := range (*value).values {
		if i != 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(oneValue.String())
	}
	builder.WriteString("]")
	return map[string]string{"firstParaValue": builder.String()}
}

type SecondaryValue struct {
	values   map[string]Value
	validate func(valueToCheck map[string]Value) error
}

func NewSecondaryValue(values map[string]Value, validateFunc func(valueToCheck map[string]Value) error) *SecondaryValue {
	return &SecondaryValue{
		values:   values,
		validate: validateFunc,
	}
}

func (value *SecondaryValue) Set(rawValue []string) error {
	// 这里输入中的每一项认为是"xxx=xxx"这样的形式
	if value == nil {
		return errors.New("no destination to store values")
	}

	num := len(rawValue)
	tempKeep := make(map[string]string)
	for i := 0; i < num; i++ {
		pairs := strings.SplitN(rawValue[i], "=", 2)
		if len(pairs) != 2 || len(pairs[0]) == 0 || len(pairs[1]) == 0 {
			return fmt.Errorf("wrong format as 'key=value': %s", rawValue[i])
		}
		if _, has := tempKeep[pairs[0]]; has {
			// key重复了
			return fmt.Errorf("duplicated key: %s", pairs[0])
		}
		if _, has := (*value).values[pairs[0]]; !has {
			// 不在预先设置的key中
			return fmt.Errorf("not expected key: %s", pairs[0])
		}
		tempKeep[pairs[0]] = pairs[1]
	}

	for subKey, subValue := range tempKeep {
		if err := (*value).values[subKey].Set([]string{subValue}); err != nil {
			return fmt.Errorf("in SecondaryValue, when set values for '%s': %s", subKey, err.Error())
		}

	}

	if (*value).validate != nil {
		if err := (*value).validate((*value).values); err != nil {
			return fmt.Errorf("validate falied!===>%s", err.Error())
		}
	}

	return nil
}

func (value *SecondaryValue) String() string {
	builder := strings.Builder{}
	// 这里遍历map的话就是无序的了，看看是否要排成有序的，排一下吧
	subKeysInOrder := make([]string, 0, len((*value).values))
	for subKey, _ := range (*value).values {
		subKeysInOrder = append(subKeysInOrder, subKey)
	}
	sort.Strings(subKeysInOrder)
	for _, subKey := range subKeysInOrder {
		builder.WriteString("\n\t")
		builder.WriteString(subKey)
		builder.WriteString(": ")
		builder.WriteString((*value).values[subKey].String())
	}
	return builder.String()
}

func (value *SecondaryValue) Get() map[string]string {
	outPutMap := make(map[string]string, len((*value).values))
	for subKey, v := range (*value).values {
		outPutMap[subKey] = v.String()
	}
	return outPutMap
}

func (value *SecondaryValue) AddSubValue(subKey string, subValue Value) error {
	if value == nil {
		return errors.New("nil value")
	}
	if (*value).values == nil {
		(*value).values = make(map[string]Value)
	}
	if _, has := (*value).values[subKey]; has {
		return fmt.Errorf("already existed subKey:%s", subKey)
	}

	(*value).values[subKey] = subValue
	return nil
}

type BoolCmdValue struct {
	Destination *bool
}

func (value *BoolCmdValue) Set(rawValue []string) error {
	if value == nil || (*value).Destination == nil {
		return errors.New("no destination to store values")
	}
	if len(rawValue) == 0 {
		return errors.New("too few values for this arg，forget to set? ")
	}
	if len(rawValue) > 1 {
		return errors.New("too many values for this arg")
	}

	asInt, err := strconv.Atoi(rawValue[0])
	if err != nil {
		return fmt.Errorf("can't use '%s' in bool setting", rawValue[0])
	}

	if !(asInt == 0 || asInt == 1) {
		return fmt.Errorf("validate falied!===> expected 0 or 1, but got '%s'", rawValue[0])
	}

	*(*value).Destination = asInt == 1
	return nil
}

func (value *BoolCmdValue) String() string {
	return fmt.Sprintf("%t", *(*value).Destination)
}

func (value *BoolCmdValue) Get() map[string]string {
	return map[string]string{"firstParaValue": fmt.Sprintf("%t", *(*value).Destination)}
}
