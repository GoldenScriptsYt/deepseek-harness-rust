use dsh_interactions::{
    ApprovalRequest, ApprovalRequestId, InteractionLimits, QuestionAnswer, QuestionOption,
    ToolCallId, UserQuestion, UserQuestionAnswer, UserQuestionRequest,
};

pub fn approval_id(value: &str) -> ApprovalRequestId {
    ApprovalRequestId::new(value).unwrap()
}

pub fn call_id(value: &str) -> ToolCallId {
    ToolCallId::new(value).unwrap()
}

pub fn approval(value: &str) -> ApprovalRequest {
    ApprovalRequest {
        id: approval_id(value),
        tool_name: "shell".to_owned(),
        call_id: Some(call_id(&format!("call-{value}"))),
        reason: Some("needs permission".to_owned()),
    }
}

pub fn question(id: &str) -> UserQuestion {
    UserQuestion {
        id: id.to_owned(),
        question: format!("Choose for {id}?"),
        detail: Some("Supporting detail".to_owned()),
        header: Some("Choice".to_owned()),
        options: Some(vec![QuestionOption {
            label: "Yes".to_owned(),
            description: Some("Proceed".to_owned()),
        }]),
        multi_select: Some(false),
    }
}

pub fn question_request(call: &str, ids: &[&str]) -> UserQuestionRequest {
    UserQuestionRequest {
        call_id: call_id(call),
        questions: ids.iter().map(|id| question(id)).collect(),
    }
}

pub fn answer(id: &str) -> QuestionAnswer {
    QuestionAnswer {
        id: id.to_owned(),
        selected: vec!["Yes".to_owned()],
        custom: None,
    }
}

pub fn answers(ids: &[&str]) -> UserQuestionAnswer {
    UserQuestionAnswer {
        answers: ids.iter().map(|id| answer(id)).collect(),
    }
}

pub fn limits() -> InteractionLimits {
    InteractionLimits {
        max_pending_approvals: 2,
        max_pending_questions: 2,
        max_recent_approvals: 2,
        max_recent_questions: 2,
    }
}
