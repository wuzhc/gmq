package gctl

type MsgSchema struct {
	MessageId int64
	Message   string
}

// push data schema
type PushSchema struct {
	Topic    string
	Delay    int32
	RouteKey string
	Message  string
}

// validate push data
func (data *PushSchema) validate() error {
	if len(data.Topic) == 0 {
		return NewClientErr(ErrParams, "topic can't empty.")
	}
	if len(data.Message) == 0 {
		return NewClientErr(ErrParams, "message can't empty.")
	}
	return nil
}

// pop data schema
type PopSchema struct {
	Topic string
	Queue string
}

// validate push data
func (data *PopSchema) validate() error {
	if len(data.Topic) == 0 {
		return NewClientErr(ErrParams, "topic can't empty.")
	}
	return nil
}

// publish data schema
type PublishSchema struct {
	Channel string
	Message string
}

// validate publish data
func (data *PublishSchema) validate() error {
	if len(data.Channel) == 0 {
		return NewClientErr(ErrParams, "channel can't empty.")
	}
	if len(data.Message) == 0 {
		return NewClientErr(ErrParams, "message can't empty.")
	}
	return nil
}

// subscribe data schema
type SubscribeSchema struct {
	Channel string
}

// validate subscribe data
func (data *SubscribeSchema) validate() error {
	if len(data.Channel) == 0 {
		return NewClientErr(ErrParams, "channel can't empty.")
	}
	return nil
}
